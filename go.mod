module github.com/kopia/kopia

go 1.17

require (
	cloud.google.com/go/storage v1.16.1
	contrib.go.opencensus.io/exporter/prometheus v0.4.0
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-blob-go v0.14.0
	github.com/GehirnInc/crypt v0.0.0-20200316065508-bb7000b8a962 // indirect
	github.com/alecthomas/kingpin v0.0.0-20200323085623-b6657d9477a6 // this is pulling master, which is newer than v2
	github.com/alecthomas/units v0.0.0-20210208195552-ff826a37aa15
	github.com/aws/aws-sdk-go v1.40.37
	github.com/chmduquesne/rollinghash v4.0.0+incompatible
	github.com/dustinkirkland/golang-petname v0.0.0-20191129215211-8e5a1ed0cff0
	github.com/efarrer/iothrottler v0.0.1
	github.com/fatih/color v1.12.0
	github.com/foomo/htpasswd v0.0.0-20200116085101-e3a90e78da9c
	github.com/frankban/quicktest v1.13.1 // indirect
	github.com/gofrs/flock v0.8.1
	github.com/golang-jwt/jwt/v4 v4.0.0
	github.com/golang/protobuf v1.5.2
	github.com/google/fswalker v0.2.1-0.20200214223026-f0e929ba4126
	github.com/google/go-cmp v0.5.6
	github.com/google/readahead v0.0.0-20161222183148-eaceba169032 // indirect
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/hanwen/go-fuse/v2 v2.1.0
	github.com/klauspost/compress v1.13.5
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/klauspost/pgzip v1.2.5
	github.com/kylelemons/godebug v1.1.0
	github.com/mattn/go-colorable v0.1.8
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.14
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/natefinch/atomic v1.0.1
	github.com/op/go-logging v0.0.0-20160315200505-970db520ece7
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.6.0
	github.com/pkg/sftp v1.13.2
	github.com/pquerna/ffjson v0.0.0-20190930134022-aa0246cd15f7 // indirect
	github.com/prometheus/client_golang v1.11.0
	github.com/sanity-io/litter v1.5.1
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/smartystreets/assertions v1.2.0 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/studio-b12/gowebdav v0.0.0-20210630100626-7ff61aa87be8
	github.com/tg123/go-htpasswd v1.1.0
	github.com/zalando/go-keyring v0.1.1
	github.com/zeebo/blake3 v0.2.0
	go.opencensus.io v0.23.0
	gocloud.dev v0.24.0
	golang.org/x/crypto v0.0.0-20210817164053-32db794688a5
	golang.org/x/exp v0.0.0-20200224162631-6cc2880d07d6
	golang.org/x/mod v0.5.0
	golang.org/x/net v0.0.0-20210825183410-e898025ed96a
	golang.org/x/oauth2 v0.0.0-20210819190943-2bc19b11175f
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210831042530-f4d43177bf5e
	golang.org/x/term v0.0.0-20210615171337-6886f2dfbf5b
	google.golang.org/api v0.56.0
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/ini.v1 v1.62.0 // indirect
	gopkg.in/kothar/go-backblaze.v0 v0.0.0-20210124194846-35409b867216
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

require (
	cloud.google.com/go v0.94.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.20 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.15 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/go-kit/log v0.1.0 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/godbus/dbus/v5 v5.0.4 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/wire v0.5.0 // indirect
	github.com/googleapis/gax-go/v2 v2.1.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.13 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.30.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/prometheus/statsd_exporter v0.22.0 // indirect
	github.com/rs/xid v1.3.0 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210831024726-fe130286e0e2 // indirect
)
