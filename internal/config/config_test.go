package config

import (
	"reflect"
	"testing"
)

func TestGet(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    *Config
		wantErr bool
	}{
		{
			name: "empty path. set defaults",
			args: args{
				path: "",
			},
			want:    defConfig,
			wantErr: false,
		},
		{
			name: "config file doesn't exist. not ok",
			args: args{
				path: "./testdata/non-existing-path",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "empty config file. set defaults",
			args: args{
				path: "./testdata/config.nodata.test.yml",
			},
			want:    defConfig,
			wantErr: false,
		},
		{
			name: "empty config file data. ok",
			args: args{
				path: "./testdata/config.nodata.test.yml",
			},
			want:    defConfig,
			wantErr: false,
		},
		{
			name: "config file parsed",
			args: args{
				path: "./testdata/config.withdata.test.yml",
			},
			want: &Config{
				Engine: EngineConfig{
					Type: "fake",
				},
				Network: NetworkConfig{
					Address:        "fake",
					MaxConnections: 1,
				},
				Logging: LoggingConfig{
					Level:  "fake",
					Output: "fake",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Get(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}
