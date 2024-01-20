# Colima Workarounds

Sometimes when building locally, this can happen:

```
2024-01-20 14:30:42.029 [testcontainers-ryuk] WARN  o.t.utility.RyukResourceReaper - Can not connect to Ryuk at 192.168.107.2:32795
java.net.SocketTimeoutException: Connect timed out
	at java.base/sun.nio.ch.NioSocketImpl.timedFinishConnect(NioSocketImpl.java:546)
	at java.base/sun.nio.ch.NioSocketImpl.connect(NioSocketImpl.java:597)
	at java.base/java.net.SocksSocketImpl.connect(SocksSocketImpl.java:327)
	at java.base/java.net.Socket.connect(Socket.java:633)
	at org.testcontainers.utility.RyukResourceReaper.lambda$null$1(RyukResourceReaper.java:105)
	at org.rnorth.ducttape.ratelimits.RateLimiter.doWhenReady(RateLimiter.java:27)
	at org.testcontainers.utility.RyukResourceReaper.lambda$maybeStart$2(RyukResourceReaper.java:101)
	at java.base/java.lang.Thread.run(Thread.java:833)
```

or something else that may be related to timeout's to MongoDB. There seem to be a problem with colima and MacOSX, it started
for me after upgrading to macOS Sonoma 14.2.1. It's not easy to resolve, but sometimes this helps:

```bash
colima stop ; colima start --vm-type vz --network-address
```

You may need to restart the terminal/intellij for it to take effect. This command cannot return `null`:

```bash
colima ls -j | jq -r '.address'
```

If this doesn't work, try restarting the computer and run the stop/start command again (don't forget to restart intellij/terminal afterwards).
If it still doesn't work, try:

1. `colima prune -a`
2. `colima delete -f`
3. `brew uninstall colima`
4. `rm -rf ~/.colima`
5. `brew install colima`
6. `colima start --vm-type vz --network-address`

Restart computer if it doesn't work..