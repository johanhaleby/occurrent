// The reason for defining a module is to allow the RetryStrategy interface to be sealed, but have an
// implementation, `RetryImpl`, in a different package.
module retry {
    exports org.occurrent.retry;
    requires kotlin.stdlib;
    requires org.jetbrains.annotations;
}