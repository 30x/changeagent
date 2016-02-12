package main

import (
  "bufio"
  "bytes"
  "fmt"
  "os"
  "strconv"
  "strings"
  "math"
  "time"
  "math/rand"
  "net/http"
  "text/template"
)

const (
  MaxRecords = 200000.0
  Collection = "records"
)

func printUsage() {
  fmt.Println("Usage:")
  fmt.Println("  loader maketenants <tenant file> <num tenants>")
  fmt.Println("  loader loadtenants <tenant file> <median records> <server url>")
}

func main() {
  if len(os.Args) < 2 {
    printUsage()
    os.Exit(2)
  }

  if os.Args[1] == "maketenants" {
    if len(os.Args) < 4 {
      printUsage()
      os.Exit(2)
    }
    count, _ := strconv.Atoi(os.Args[3])
    os.Exit(createTenants(os.Args[2], count))

  } else if os.Args[1] == "loadtenants" {
    if len(os.Args) < 5 {
      printUsage()
      os.Exit(2)
    }
    median, _ := strconv.Atoi(os.Args[3])
    os.Exit(loadTenants(os.Args[2], median, os.Args[4]))

  } else {
    printUsage()
    os.Exit(2)
  }
}

func createTenants(fileName string, count int) int {
  f, err := os.OpenFile(fileName, os.O_CREATE | os.O_TRUNC | os.O_RDWR, 0666)
  if err != nil {
    fmt.Printf("Error opening tenant file: %s", err)
    return 3
  }
  defer f.Close()

  for i := 0; i < count; i++ {
    n := makeName()
    fmt.Fprintf(f, "%s\n", n)
  }
  return 0
}

func loadTenants(fileName string, med int, uri string) int {
  median := float64(med) / MaxRecords

  tenants, err := loadTenantFile(fileName)
  if err != nil {
    fmt.Printf("Can't load tenant file: %s\n", err)
    return 3
  }

  fmt.Printf("Going to load %d tenants\n", len(tenants))

  // Load everything in random order.
  desired := make(map[string]int)
  actual := make(map[string]int)

  for _, tenant := range(tenants) {
    norm := math.Abs(rand.NormFloat64()) * median
    desired[tenant] = int(norm * MaxRecords)
    actual[tenant] = -1
  }

  tot := 0
  for k := range(desired) {
    tot += desired[k]
  }
  fmt.Printf("Going to create %d records\n", tot)
  start := time.Now()

  tot = 0
  for len(actual) > 0 {
    ten := tenants[rand.Intn(len(tenants))]
    if actual[ten] == 0 {
      continue
    }
    if desired[ten] == 0 {
      delete(actual, ten)
      continue
    }
    if actual[ten] == -1 {
      err := createTenant(ten, uri)
      if err == nil {
        actual[ten] = 1
      } else {
        fmt.Printf("Error creating tenant: %s\n", err)
        time.Sleep(2 * time.Second)
        continue
      }
    }

    err := insertRecord(ten, uri)
    if err == nil {
      actual[ten] = actual[ten] + 1
      if actual[ten] == desired[ten] {
        delete(actual, ten)
      }
      tot++
      if (tot % 1000) == 0 && tot > 0 {
        fmt.Printf("%d...\n", tot)
      }
    } else {
      fmt.Printf("Error inserting record: %s\n", err)
      time.Sleep(2 * time.Second)
      continue
    }
  }

  end := time.Now()
  elapsed := end.Sub(start)

  fmt.Printf("Inserted %d records in %s\n", tot, elapsed)
  fmt.Printf("%.2f records / second\n", float64(tot) / elapsed.Seconds())
  fmt.Printf("%.4f milliseconds / record\n", float64(elapsed.Nanoseconds() / int64(tot)) / float64(time.Millisecond))

  return 0
}

func createTenant(name string, base string) error {
  url := fmt.Sprintf("%s/tenants", base)
  values := make(map[string][]string)
  values["tenant"] = []string{name}

  resp, err := http.PostForm(url, values)
  if err != nil { return err }
  if resp.StatusCode != 200 {
    return fmt.Errorf("HTTP error creating tenant: %d", resp.StatusCode)
  }
  resp.Body.Close()

  url = fmt.Sprintf("%s/tenants/%s/collections", base, name)
  values = make(map[string][]string)
  values["collection"] = []string{Collection}

  resp, err = http.PostForm(url, values)
  if err != nil { return err }
  if resp.StatusCode != 200 {
    return fmt.Errorf("HTTP error creating collection: %d", resp.StatusCode)
  }
  resp.Body.Close()
  return nil
}

type EntryValues struct {
  Tenant string
  Collection string
  Key string
}

const EntryTemplate =
  "{\"tenant\": \"{{.Tenant}}\", \"collection\": \"{{.Collection}}\", \"key\": \"{{.Key}}\"," +
  "\"data\":{\"name\":\"{{.Key}}\", \"info\": \"Hello, there!\"}}"
var entry = template.Must(template.New("entry").Parse(EntryTemplate))

func insertRecord(tenant string, base string) error {
  url := fmt.Sprintf("%s/changes", base)
  v := EntryValues{
    Tenant: tenant,
    Collection: Collection,
    Key: makeName(),
  }
  entryBytes := bytes.Buffer{}
  entry.Execute(&entryBytes, v)

  resp, err := http.Post(url, "application/json", bytes.NewReader(entryBytes.Bytes()))
  if err != nil { return err }
  if resp.StatusCode != 200 {
    return fmt.Errorf("HTTP error creating tenant: %d", resp.StatusCode)
  }
  resp.Body.Close()

  return nil
}

func loadTenantFile(fileName string) ([]string, error) {
  f, err := os.Open(fileName)
  if err != nil { return nil, err }

  var tenants []string
  rdr := bufio.NewReader(f)

  for {
    tenant, _ := rdr.ReadString('\n')
    if tenant == "" {
      break
    }
    tenants = append(tenants, strings.TrimSpace(tenant))
  }
  return tenants, nil
}

// Random tenant name between 2 and 20 characters
func makeName() string {
  nl := rand.Intn(18) + 2
  var n []uint8
  for i := 0; i < nl; i++ {
    c := rand.Intn(26) + 97
    n = append(n, uint8(c))
  }
  return string(n)
}