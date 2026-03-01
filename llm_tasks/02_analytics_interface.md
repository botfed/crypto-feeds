
# Overview


The purpose of this rust library is to provide low latency connections to different crypto exchanges decentralized or centralized. 


It accomplishes this via the different exchange specific listen bbo functions for spot and perps that stream market data into the market_data.rs structs.


However, for downsgtream consumer it is invaluable to have an analytics interface. 

We alredy implemented tick based ring buffers. 

Now we need an interface that will be in charge of :


1. GIving access to these tick based buffers on a per symbol id and exchange basis.
2. Take snapshots of all these ring buffers into a snapshot buffer default every 100ms and add log return between snaps to the stored fields. 
3. Provide analytics functions like mean, median or stdev over last N snaps or ticks of all fields or a specific fields. 

4. Provide this interface to the pypbindings as well. 




Any other thoughts?


These things will be used downstream in market making and cex dex arb bots. 