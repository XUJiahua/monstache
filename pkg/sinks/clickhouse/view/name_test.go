package view

import "testing"

func TestConvertToClickhouseTable(t *testing.T) {
	type args struct {
		ns     string
		prefix string
		suffix string
	}
	var tests = []struct {
		name string
		args args
		want string
	}{
		{
			args: args{
				ns:     "config-bkk.trans",
				prefix: "",
				suffix: "",
			},
			want: "config_bkk_trans",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ConvertToClickhouseTable(tt.args.ns, tt.args.prefix, tt.args.suffix); got != tt.want {
				t.Errorf("ConvertToClickhouseTable() = %v, want %v", got, tt.want)
			}
		})
	}
}
