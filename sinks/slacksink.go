/*
Copyright 2018 Oracle.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sinks

import (
	"bytes"
	"encoding/json"
	"github.com/eapache/channels"
	"github.com/golang/glog"
	"net/http"

	"k8s.io/api/core/v1"
)

/*
The Slack sink is a sink that sends events over HTTP webhook
*/

// SlackSink wraps an HTTP endpoint that messages should be sent to.
type SlackSink struct {
	SinkURL string
	eventCh    channels.Channel
}

// NewSlackSink constructs a new SlackSink given a sink URL and buffer size
func NewSlackSink(sinkURL string, overflow bool, bufferSize int) *SlackSink {
	h := &SlackSink{
		SinkURL: sinkURL,
	}

	if overflow {
		h.eventCh = channels.NewOverflowingChannel(channels.BufferCap(bufferSize))
	} else {
		h.eventCh = channels.NewNativeChannel(channels.BufferCap(bufferSize))
	}

	return h
}

// UpdateEvents implements the EventSinkInterface. It really just writes the
// event data to the event OverflowingChannel, which should never block.
// Messages that are buffered beyond the bufferSize specified for this SlackSink
// are discarded.
func (h *SlackSink) UpdateEvents(eNew *v1.Event, eOld *v1.Event) {
	h.eventCh.In() <- NewEventData(eNew, eOld)
}

// Run sits in a loop, waiting for data to come in through h.eventCh,
// and forwarding them to the HTTP sink. If multiple events have happened
// between loop iterations, it puts all of them in one request instead of
// making a single request per event.
func (h *SlackSink) Run(stopCh <-chan bool) {
loop:
	for {
		select {
		case e := <-h.eventCh.Out():
			var evt EventData
			var ok bool
			if evt, ok = e.(EventData); !ok {
				glog.Warningf("Invalid type sent through event channel: %T", e)
				continue loop
			}

			// Start with just this event...
			arr := []EventData{evt}

			// Consume all buffered events into an array, in case more have been written
			// since we last forwarded them
			numEvents := h.eventCh.Len()
			for i := 0; i < numEvents; i++ {
				e := <-h.eventCh.Out()
				if evt, ok = e.(EventData); ok {
					arr = append(arr, evt)
				} else {
					glog.Warningf("Invalid type sent through event channel: %T", e)
				}
			}

			h.drainEvents(arr)
		case <-stopCh:
			break loop
		}
	}
}

type SlackKubeEvent struct {
	Channel string `json:"channel"`
	Username string `json:"username"`
	Text string `json:"text"`
	IconEmoji string `json:"icon_emoji"`
}

func (h *SlackSink) drainEvents(events []EventData) {

	for _, evt := range events {
		se := SlackKubeEvent{
			Channel: "#events",
			Text: evt.Event.Message,
			Username: "eventrouter",
			IconEmoji: ":ghost:",
		}

		eventJson, err := json.Marshal(se)
		if err != nil {
			glog.Errorf("error json marshal: %v - err: %v", se, err)
		}

		resp, err := http.DefaultClient.Post( h.SinkURL, "application/json", bytes.NewReader(eventJson) )
		if err != nil {
			glog.Warningf(err.Error())
			return
		}

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			glog.Warningf("Got HTTP code %v from %v - resp: %v", resp.StatusCode, h.SinkURL, resp)
		}

	}

}
