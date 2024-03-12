package common

import (
	"testing"
)

func Test_parseTime(t *testing.T) {
	type args struct {
		tStr string
	}
	tests := []struct {
		name  string
		args  args
		want  int64
		want1 bool
	}{
		{
			name: "2024-01-20T16:00:43.516Z",
			args: args{
				tStr: "2024-01-20T16:00:43.516Z",
			},
			want:  1705766443516,
			want1: true,
		},
		{
			name: "2024-01-20T16:00:43Z",
			args: args{
				tStr: "2024-01-20T16:00:43Z",
			},
			want:  1705766443000,
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := parseTime(tt.args.tStr)
			if got != tt.want {
				t.Errorf("parseTime() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("parseTime() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
