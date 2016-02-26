package storage

import (
  "github.com/satori/go.uuid"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

var _ = Describe("Tenant Index Test", func() {
  It("Basic tenant index", func() {
    tenant1 := uuid.NewV4()
    tenant2 := uuid.NewV4()
    tenant3 := uuid.NewV4()

    var empty []uint64

    verifyTenantIndex(&tenant1, empty)
    verifyTenantIndex(&tenant2, empty)
    verifyTenantIndex(&tenant3, empty)

    entry1 := Entry{
      Index: 1,
      Tenant: &tenant1,
      Data: []byte("Hello"),
    }

    err := indexTestDb.CreateTenantEntry(&entry1)
    Expect(err).Should(Succeed())
    verifyTenantIndex(&tenant1, []uint64{1})
    verifyTenantIndex(&tenant2, empty)
    verifyTenantIndex(&tenant3, empty)

    entry2 := Entry{
      Index: 2,
      Tenant: &tenant2,
      Data: []byte("Hello"),
    }
    err = indexTestDb.CreateTenantEntry(&entry2)
    Expect(err).Should(Succeed())

    entry3 := Entry{
      Index: 3,
      Tenant: &tenant1,
      Data: []byte("Hello"),
    }
    err = indexTestDb.CreateTenantEntry(&entry3)
    Expect(err).Should(Succeed())

    verifyTenantIndex(&tenant1, []uint64{1, 3})
    verifyTenantIndex(&tenant2, []uint64{2})
    verifyTenantIndex(&tenant3, empty)

    found1, err := indexTestDb.GetTenantEntry(&tenant1, 1)
    Expect(err).Should(Succeed())
    Expect(found1.Index).Should(Equal(entry1.Index))

    found2, err := indexTestDb.GetTenantEntry(&tenant2, 2)
    Expect(err).Should(Succeed())
    Expect(found2.Index).Should(Equal(entry2.Index))

    found3, err := indexTestDb.GetTenantEntry(&tenant1, 3)
    Expect(err).Should(Succeed())
    Expect(found3.Index).Should(Equal(entry3.Index))

    entries, err := indexTestDb.GetTenantEntries(&tenant1, 0, 1)
    Expect(err).Should(Succeed())
    Expect(len(entries)).Should(Equal(1))
    Expect(entries[0].Index).Should(Equal(entry1.Index))

    entries, err = indexTestDb.GetTenantEntries(&tenant1, 1, 1)
    Expect(err).Should(Succeed())
    Expect(len(entries)).Should(Equal(1))
    Expect(entries[0].Index).Should(Equal(entry3.Index))

    entries, err = indexTestDb.GetTenantEntries(&tenant1, 3, 1)
    Expect(err).Should(Succeed())
    Expect(entries).Should(BeEmpty())
  })
})

func verifyTenantIndex(id *uuid.UUID, ixes []uint64) {
  var found []uint64

  entries, err := indexTestDb.GetTenantEntries(id, 0, 1000)
  Expect(err).Should(Succeed())

  for _, e := range(entries) {
    found = append(found, e.Index)
  }
  Expect(found).Should(Equal(ixes))
}
