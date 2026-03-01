
# Overview


The purpose of this rust library is to provide low latency connections to different crypto exchanges decentralized or centralized. 


It accomplishes this via the different exchange specific listen bbo functions for spot and perps that stream market data into the market_data.rs structs.


However, in any trading system it is invaluable to keep ring buffers of these data feeds snapped at different frequencies. 

This will be very useful for consumers of the python bindings.

We want to plan out the architecture for how to achieve these goals. 

There should be ring buffers of some kind of default large MAX_LEN (modern systems with several gigs of ram should easily handle a max len in the hundred k or millions).
THere shiould be a snapshot frequency which iis user specified in nanos and maybe has a default of 100ms. 

Forther more market data itself should perhaps be updated to be a ring buffer that keeps the last N bbo updates ... why not?? 



Tasks ...

1. Tick basedf ring buffers 

Plan the uprage for the market data structures to be ring buffers with MAX_LEN ~ 1M
Ensure this doesn't screw up our blazing fast latency which should be in the NS range.
The locks should be atomics if possible.
Only one writer per ring buffer per exchange per symbol id. 


2. Snapshot buffers w/ analytics. 


These guys should snap with the user specified frequency.

The interface should expose some basic analytics functions like get_miquote_twap over past duration, mean spread, get log return series, etc. 


The default max len should be chosen so that ram usage is less than 1GB for a typical use case of up to one hundred symbols. 


We envision this being used from python, and having one interface from which these things can be accessed on a per symbol id and exchange basis. 


