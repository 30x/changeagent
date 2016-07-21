package auth

import (
	"io"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAuth(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Auth Suite")
}

var _ = Describe("Authentication tests", func() {
	It("Good file", func() {
		s := NewAuthStore()
		err := s.Load("./testfiles/pw1")
		Expect(err).Should(Succeed())
		Expect(s.Authenticate("foo@bar.com", "baz")).Should(BeTrue())
		// Repeated authentication to test caching logic
		Expect(s.Authenticate("foo@bar.com", "baz")).Should(BeTrue())
		Expect(s.Authenticate("bar@bar.com", "foobarbazfoobarbaz")).Should(BeTrue())
		Expect(s.Authenticate("bar@bar.com", "foobarbazfoobarbaz")).Should(BeTrue())
		// Wrong PW
		Expect(s.Authenticate("foo@bar.com", "bar")).ShouldNot(BeTrue())
		// Not in the file
		Expect(s.Authenticate("notthere", "atall")).ShouldNot(BeTrue())
		// In the file but not bcrypt
		Expect(s.Authenticate("bad@foo.com", "badpass")).ShouldNot(BeTrue())
		Expect(s.Authenticate("bad2@foo.com", "badpass")).ShouldNot(BeTrue())
		Expect(s.Authenticate("bad3@foo.com", "badpass")).ShouldNot(BeTrue())
	})

	It("Reload", func() {
		s := NewAuthStore()
		err := s.Load("./testfiles/pw1")
		Expect(err).Should(Succeed())
		Expect(s.Authenticate("foo@bar.com", "baz")).Should(BeTrue())

		err = s.Load("./testfiles/pw2")
		Expect(err).Should(Succeed())
		Expect(s.Authenticate("foo@bar.com", "newpassword")).Should(BeTrue())
		Expect(s.Authenticate("foo@bar.com", "baz")).Should(BeFalse())
	})

	It("Reload Automatically", func() {
		os.Remove("./testfiles/tpw")
		err := copyFile("./testfiles/pw1", "./testfiles/tpw")
		Expect(err).Should(Succeed())
		defer os.Remove("./testfiles/tpw")

		s := NewAuthStore()
		defer s.Close()
		err = s.Load("./testfiles/tpw")
		Expect(err).Should(Succeed())
		Expect(s.Authenticate("foo@bar.com", "baz")).Should(BeTrue())

		err = s.Watch(100 * time.Millisecond)
		Expect(err).Should(Succeed())

		time.Sleep(1 * time.Second)
		err = os.Remove("./testfiles/tpw")
		Expect(err).Should(Succeed())
		err = copyFile("./testfiles/pw2", "./testfiles/tpw")
		Expect(err).Should(Succeed())

		Eventually(func() bool {
			if s.Authenticate("foo@bar.com", "baz") {
				return false
			}
			return s.Authenticate("foo@bar.com", "newpassword")
		}, 2*time.Second).Should(BeTrue())
	})

	Measure("Measure Lookup", func(b Benchmarker) {
		s := NewAuthStore()
		err := s.Load("./testfiles/pw1")
		Expect(err).Should(Succeed())

		for i := 0; i < 1000; i++ {
			b.Time("authenticate", func() {
				ok := s.Authenticate("foo@bar.com", "baz")
				Expect(ok).Should(BeTrue())
			})
		}
	}, 100)
})

func copyFile(src, dst string) error {
	sf, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sf.Close()
	df, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer df.Close()

	_, err = io.Copy(df, sf)
	return err
}
