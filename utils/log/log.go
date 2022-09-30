package log

import (
	cfg "project/config"
	"time"

	nested "github.com/antonfisher/nested-logrus-formatter"
	log "github.com/sirupsen/logrus"
)

func LoggerInit(ctx *cfg.Context) {
	ctx.Logger = log.New()
	if ctx.Debug {
		ctx.Logger.SetLevel(log.DebugLevel)
	} else {
		ctx.Logger.SetLevel(log.InfoLevel)
	}

	// ctx.Logger.SetReportCaller(true)
	ctx.Logger.SetFormatter(&nested.Formatter{
		HideKeys:        true,
		TimestampFormat: time.StampNano,
	})
}
