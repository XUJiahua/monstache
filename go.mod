module github.com/rwynn/monstache/v6

// https://github.com/rwynn/gtm/compare/master...XUJiahua:gtm:resumable?expand=1
replace github.com/rwynn/gtm/v2 => github.com/XUJiahua/gtm/v2 v2.0.0-20240313085840-ad21f1e96a0c

//replace github.com/rwynn/gtm/v2 => ../gtm

require (
	github.com/BurntSushi/toml v1.3.2
	github.com/aws/aws-sdk-go v1.44.239
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf
	github.com/evanphx/json-patch v5.6.0+incompatible
	github.com/fsnotify/fsnotify v1.5.1
	github.com/olivere/elastic/v7 v7.0.31
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.18.0
	github.com/robertkrimen/otto v0.0.0-20211024170158-b87d35c0b86f
	github.com/rwynn/gtm/v2 v2.1.2
	github.com/segmentio/kafka-go v0.4.47
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.8.2
	go.mongodb.org/mongo-driver v1.10.6
	gopkg.in/Graylog2/go-gelf.v2 v2.0.0-20191017102106-1550ee647df0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

go 1.13
