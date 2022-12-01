package blockgen

import (
	"bytes"
	"context"
	"fmt"
	"github.com/thanos-io/thanos/pkg/model"
	"gopkg.in/yaml.v2"
	"testing"
	"time"
)

func TestContinuousFromTemplate(t *testing.T) {
	type args struct {
		ranges []time.Duration
		spt    PlanTemplate
	}
	tests := []struct {
		name string
		args args
		want PlanFn
	}{
		{
			name: "single",
			args: args{
				ranges: []time.Duration{time.Hour},
				spt: PlanTemplate{
					BaseName:    "test",
					SeriesCount: 1,
					Targets:     1,
					Labels: []Label{
						{
							Key:         "a",
							Cardinality: 2,
						},
						{
							Key:         "b",
							Cardinality: 1,
						},
					},
					ScrapeIntervalSeconds: 30,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			planFn := ContinuousFromTemplate(tt.args.ranges, tt.args.spt)

			maxTime := time.Now().Add(time.Duration(-30) * time.Minute)
			maxT := model.TimeOrDurationValue{
				Time: &maxTime,
			}

			buf := bytes.Buffer{}
			enc := yaml.NewEncoder(&buf)

			got := planFn(context.Background(), maxT, nil, func(spec BlockSpec) error { return enc.Encode(spec) })
			fmt.Println(got)
		})
	}
}
