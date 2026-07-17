# Repository specific instructions #

When adding more code generated functionality, prefer adding it to rt/ and importing it rather than generating it.

Pass SWIFT_ARGS=--disable-sandbox whenever using Swift, as Swift sandbox does not work with the tests here.
