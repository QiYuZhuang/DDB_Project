package log

import (
	cfg "project/config"

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

	ctx.Logger.SetFormatter(
		&nested.Formatter{
			HideKeys:        true,
			TimestampFormat: "2006-01-02 15:04:05",
		})
	ctx.Logger.SetReportCaller(true)
}
