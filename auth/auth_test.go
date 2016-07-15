package auth

import (
	"testing"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAuth(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Auth Suite")
}

var _ = Describe("Authentication tests", func() {
	var pw *Store

	BeforeEach(func() {
		pw = NewAuthStore()
	})

	It("Empty", func() {
		res := pw.Authenticate("foo", "bar")
		Expect(res).ShouldNot(BeTrue())
		Expect(pw.IsEmpty()).Should(BeTrue())
	})

	It("Add user", func() {
		pw.SetUser("foo", "bar")
		Expect(pw.IsEmpty()).ShouldNot(BeTrue())
		res := pw.Authenticate("foo", "bar")
		Expect(res).Should(BeTrue())
		res = pw.Authenticate("foo", "baz")
		Expect(res).ShouldNot(BeTrue())
		res = pw.Authenticate("bar", "bar")
		Expect(res).ShouldNot(BeTrue())
		pw.DeleteUser("foo")
		res = pw.Authenticate("foo", "bar")
		Expect(res).ShouldNot(BeTrue())
	})

	It("Basic Auth", func() {
		pw.SetUser("foo", "bar")
		res := pw.AuthenticateBasic("Basic Zm9vOmJhcg==")
		Expect(res).Should(BeTrue())
		res = pw.AuthenticateBasic("Basic  Zm9vOmJhcg==")
		Expect(res).Should(BeTrue())
		res = pw.AuthenticateBasic("Basic Zm9vOmJheg==")
		Expect(res).ShouldNot(BeTrue())
		res = pw.AuthenticateBasic("Basic Zm9vYmFy")
		Expect(res).ShouldNot(BeTrue())
		res = pw.AuthenticateBasic("Basicck Zm9vOmJhcg==")
		Expect(res).ShouldNot(BeTrue())
		res = pw.AuthenticateBasic("Zm9vOmJhcg==")
		Expect(res).ShouldNot(BeTrue())
	})

	It("Encode", func() {
		pw.SetUser("foo", "bar")
		pw.SetUser("user", "password123!")
		enc := pw.Encode()
		dec, err := DecodeAuthStore(enc)
		Expect(err).Should(Succeed())
		res := dec.Authenticate("foo", "bar")
		Expect(res).Should(BeTrue())
		res = dec.Authenticate("user", "password123!")
		Expect(res).Should(BeTrue())
	})

	It("Stress", func() {
		err := quick.Check(func(u, p string) bool {
			return testPass(u, p, pw)
		}, nil)
		Expect(err).Should(Succeed())
	})

	Measure("Measure Lookup", func(b Benchmarker) {
		pw.SetUser("user2", "password123!")
		for i := 0; i < 100; i++ {
			b.Time("authenticate", func() {
				ok := pw.Authenticate("user2", "password123!")
				Expect(ok).Should(BeTrue())
			})
		}
	}, 10)
})

func testPass(u, p string, pw *Store) bool {
	pw.SetUser(u, p)
	return pw.Authenticate(u, p)
}
