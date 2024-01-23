### NordVPN

If NordVPN is configured to "stay invisible at the local network", then TestContainers (Ryuk) won't work and you'll get errors like this:

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

You can change this setting by loading up the NordVPN app, then goto Settings -> Kill Switch -> and disable "Stay invisible at the local network" (ctrl+alt+k).

If it still doesn't work, you may need to disconnect NordVPN, restart Rancher Desktop, restart Warp and then try again (sometimes a couple of times).