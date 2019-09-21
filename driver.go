package gnats

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/driver"
)

var errNotInitialized = errors.New("gnats: topic not initialized")

func init() {
	o := new(defaultDialer)
	pubsub.DefaultURLMux().RegisterTopic(Scheme, o)
	pubsub.DefaultURLMux().RegisterSubscription(Scheme, o)
}

// defaultDialer dials a default NATS server based on the environment
// variable "NATS_STREAMING_SERVER_URL".
type defaultDialer struct {
	init   sync.Once
	opener *URLOpener
	err    error
}

func (o *defaultDialer) defaultConn(ctx context.Context) (*URLOpener, error) {
	o.init.Do(func() {
		serverURL := os.Getenv("NATS_STREAMING_SERVER_URL")
		if serverURL == "" {
			o.err = errors.New("NATS_STREAMING_SERVER_URL environment variable not set")
			return
		}
		clusterID := os.Getenv("NATS_STREAMING_CLUSTER_ID")
		if serverURL == "" {
			o.err = errors.New("NATS_STREAMING_CLUSTER_ID environment variable not set")
			return
		}
		clientID := os.Getenv("NATS_STREAMING_CLIENT_ID")
		if serverURL == "" {
			o.err = errors.New("NATS_STREAMING_CLIENT_ID environment variable not set")
			return
		}
		conn, err := stan.Connect(clusterID, clientID, stan.NatsURL(serverURL))
		if err != nil {
			o.err = fmt.Errorf("failed to dial NATS_STREAMING_SERVER_URL %q: %v", serverURL, err)
			return
		}
		o.opener = &URLOpener{Connection: conn}
	})
	return o.opener, o.err
}

func (o *defaultDialer) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	opener, err := o.defaultConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open topic %v: failed to open default connection: %v", u, err)
	}
	return opener.OpenTopicURL(ctx, u)
}

func (o *defaultDialer) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	opener, err := o.defaultConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open subscription %v: failed to open default connection: %v", u, err)
	}
	return opener.OpenSubscriptionURL(ctx, u)
}

// Scheme is the URL scheme natspubsub registers its URLOpeners under on pubsub.DefaultMux.
const Scheme = "natsstr"

// URLOpener opens NATS URLs like "natsstr://mysubject".
//
// The URL host+path is used as the subject.
//
// No query parameters are supported.
type URLOpener struct {
	// Connection to use for communication with the server.
	Connection stan.Conn
	// TopicOptions specifies the options to pass to OpenTopic.
	TopicOptions TopicOptions
	// SubscriptionOptions specifies the options to pass to OpenSubscription.
	SubscriptionOptions stan.SubscriptionOptions
}

// OpenTopicURL opens a pubsub.Topic based on u.
func (o *URLOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	for param := range u.Query() {
		return nil, fmt.Errorf("open topic %v: invalid query parameter %s", u, param)
	}
	subject := path.Join(u.Host, u.Path)
	return OpenTopic(o.Connection, subject, &o.TopicOptions)
}

// OpenSubscriptionURL opens a pubsub.Subscription based on u.
func (o *URLOpener) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	queue := u.Query().Get("queue")
	for param := range u.Query() {
		if param != "queue" {
			return nil, fmt.Errorf("open subscription %v: invalid query parameter %s", u, param)
		}
	}
	subject := path.Join(u.Host, u.Path)
	return OpenSubscription(o.Connection, subject, queue)
}

type topic struct {
	nc   stan.Conn
	subj string
}

type TopicOptions struct {
}

// OpenTopic returns a *pubsub.Topic for use with NATS Streaming Server.
// The subject is the NATS Subject; for more info, see
// https://nats.io/documentation/writing_applications/subjects.
func OpenTopic(nc stan.Conn, subject string, _ *TopicOptions) (*pubsub.Topic, error) {
	dt, err := openTopic(nc, subject)
	if err != nil {
		return nil, err
	}
	return pubsub.NewTopic(dt, nil), nil
}

func openTopic(nc stan.Conn, subject string) (driver.Topic, error) {
	if nc == nil {
		return nil, errors.New("gnats: stan.Conn is required")
	}

	return &topic{nc, subject}, nil
}

func (t *topic) SendBatch(ctx context.Context, msgs []*driver.Message) error {
	if t == nil || t.nc == nil {
		return errNotInitialized
	}

	for _, m := range msgs {
		if err := ctx.Err(); err != nil {
			return err
		}

		payload, err := encodeMessage(m)
		if err != nil {
			return err
		}

		if m.BeforeSend != nil {
			asFunc := func(i interface{}) bool { return false }
			if err := m.BeforeSend(asFunc); err != nil {
				return err
			}
		}

		if err := t.nc.Publish(t.subj, payload); err != nil {
			return err
		}
	}

	return nil
}

func (t *topic) IsRetryable(err error) bool {
	return false
}

func (t *topic) As(i interface{}) bool {
	c, ok := i.(*stan.Conn)
	if !ok {
		return false
	}
	*c = t.nc
	return true
}

func (t *topic) ErrorAs(error, interface{}) bool {
	return false
}

func (t *topic) ErrorCode(err error) gcerrors.ErrorCode {
	switch err {
	case nil:
		return gcerrors.OK
	case context.Canceled:
		return gcerrors.Canceled
	case errNotInitialized:
		return gcerrors.NotFound
	case nats.ErrBadSubject, stan.ErrBadConnection:
		return gcerrors.FailedPrecondition
	case nats.ErrAuthorization:
		return gcerrors.PermissionDenied
	case nats.ErrMaxPayload, nats.ErrReconnectBufExceeded:
		return gcerrors.ResourceExhausted
	case stan.ErrTimeout:
		return gcerrors.DeadlineExceeded
	}
	return gcerrors.Unknown
}

func (t *topic) Close() error {
	return nil
}

type subscription struct {
	nc       stan.Conn
	sub      stan.Subscription
	ack      bool
	messages []*stan.Msg
	sync.Mutex
}

// OpenSubscription returns a *pubsub.Subscription representing a NATS subscription.
// The subject is the NATS Subject to subscribe to; for more info, see
// https://nats.io/documentation/writing_applications/subjects.
func OpenSubscription(nc stan.Conn, subject, queue string, oo ...stan.SubscriptionOption) (*pubsub.Subscription, error) {
	ds, err := openSubscription(nc, subject, queue, oo)
	if err != nil {
		return nil, err
	}
	return pubsub.NewSubscription(ds, nil, nil), nil
}

func openSubscription(nc stan.Conn, subject, queue string, oo []stan.SubscriptionOption) (driver.Subscription, error) {
	opt := stan.DefaultSubscriptionOptions
	for _, o := range oo {
		if err := o(&opt); err != nil {
			return nil, err
		}
	}

	s := &subscription{nc: nc, ack: opt.ManualAcks}
	handler := func(msg *stan.Msg) {
		s.Lock()
		s.messages = append(s.messages, msg)
		s.Unlock()
	}

	var err error
	if queue != "" {
		s.sub, err = nc.QueueSubscribe(subject, queue, handler)
	} else {
		s.sub, err = nc.Subscribe(subject, handler)
	}
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *subscription) ReceiveBatch(ctx context.Context, maxMessages int) ([]*driver.Message, error) {
	if s == nil || s.nc == nil {
		return nil, stan.ErrBadSubscription
	}

	mc := maxMessages
	s.Lock()
	if mc > len(s.messages) {
		mc = len(s.messages)
	}
	msgs := s.messages[:mc]
	s.messages = s.messages[mc:]
	s.Unlock()

	dms := make([]*driver.Message, 0, len(msgs))

	for _, m := range msgs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		dm, err := decode(m)
		if err != nil {
			return nil, err
		}
		dms = append(dms, dm)
	}

	return dms, nil
}

func (s *subscription) SendAcks(ctx context.Context, ackIDs []driver.AckID) error {
	if !s.ack { // Auto acknowledge is set
		return nil
	}

	for _, a := range ackIDs {
		if err := a.(*stan.Msg).Ack(); err != nil {
			return err
		}
	}

	return nil
}

func (s *subscription) CanNack() bool {
	return false
}

func (s *subscription) SendNacks(ctx context.Context, ackIDs []driver.AckID) error {
	panic("unreachable")
}

func (s *subscription) IsRetryable(err error) bool {
	return false
}

func (s *subscription) As(i interface{}) bool {
	c, ok := i.(*stan.Subscription)
	if !ok {
		return false
	}
	*c = s.sub
	return true
}

func (s *subscription) ErrorAs(error, interface{}) bool {
	return false
}

func (s *subscription) ErrorCode(err error) gcerrors.ErrorCode {
	switch err {
	case nil:
		return gcerrors.OK
	case context.Canceled:
		return gcerrors.Canceled
	case errNotInitialized, nats.ErrBadSubscription:
		return gcerrors.NotFound
	case nats.ErrBadSubject, nats.ErrTypeSubscription:
		return gcerrors.FailedPrecondition
	case nats.ErrAuthorization:
		return gcerrors.PermissionDenied
	case nats.ErrMaxMessages, nats.ErrSlowConsumer:
		return gcerrors.ResourceExhausted
	case nats.ErrTimeout:
		return gcerrors.DeadlineExceeded
	}
	return gcerrors.Unknown
}

func (s *subscription) Close() error {
	return s.sub.Close()
}

func decode(msg *stan.Msg) (*driver.Message, error) {
	if msg == nil {
		return nil, nats.ErrInvalidMsg
	}
	var dm driver.Message
	if err := decodeMessage(msg.Data, &dm); err != nil {
		return nil, err
	}
	dm.AckID = msg
	dm.AsFunc = messageAsFunc(msg)
	return &dm, nil
}

func messageAsFunc(msg *stan.Msg) func(interface{}) bool {
	return func(i interface{}) bool {
		p, ok := i.(**stan.Msg)
		if !ok {
			return false
		}
		*p = msg
		return true
	}
}

func encodeMessage(dm *driver.Message) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if len(dm.Metadata) == 0 {
		return dm.Body, nil
	}
	if err := enc.Encode(dm.Metadata); err != nil {
		return nil, err
	}
	if err := enc.Encode(dm.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeMessage(data []byte, dm *driver.Message) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&dm.Metadata); err != nil {
		// This may indicate a normal NATS message, so just treat as the body.
		dm.Metadata = nil
		dm.Body = data
		return nil
	}
	return dec.Decode(&dm.Body)
}
