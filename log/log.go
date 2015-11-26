package log

import (
  "log"
  "os"
)

var debugOn bool
var logger *log.Logger = log.New(os.Stdout, "ferry ", log.LstdFlags)

func InitDebug(d bool) {
  debugOn = d
}

func Debug(msg string) {
  if (debugOn) {
    logger.Println(msg)
  }
}

func Debugf(format string, v ...interface{}) {
  if (debugOn) {
    logger.Printf(format + "\n", v...)
  }
}

func Infof(format string, v ...interface{}) {
  logger.Printf(format + "\n", v...)
}

func Info(msg string) {
  logger.Println(msg)
}

func Panic(msg string) {
  logger.Panic(msg)
}

func Panicf(format string, v ...interface{}) {
  logger.Panicf(format + "\n", v...)
}
