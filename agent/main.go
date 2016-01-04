package main

import (
  "fmt"
  "flag"
  "os"
)

const (
  DefaultNode = 1
  DefaultPort = 8080
)

func main() {
  var nodeId int
  var port int
  var dbDir string
  var discoveryFile string

  flag.IntVar(&nodeId, "id", DefaultNode, "Node ID. Must be in discovery data.")
  flag.IntVar(&port, "p", DefaultPort, "Port to listen on.")
  flag.StringVar(&dbDir, "d", "", "Directory in which to place data.")
  flag.StringVar(&discoveryFile, "s", "", "File from which to read list of peers")

  flag.Parse()
  if !flag.Parsed() {
    flag.PrintDefaults()
    os.Exit(2)
  }

  if dbDir == "" {
    fmt.Println("Database directory must be specified.")
    flag.PrintDefaults()
    os.Exit(3)
  }
  if discoveryFile == "" {
    fmt.Println("Discovery file name must be specified.")
    flag.PrintDefaults()
    os.Exit(4)
  }
}
