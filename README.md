# ds5220-app
App for Advanced Cloud Computing designed to monitor tide levels.

- A summary of your data source:
    My data comes from water level measurements by the National Oceanic and Atmospheric Association (NOAA) in 4 different locations. They use a massive network of instruments called the National Water Level Observation Network (NWLON). These instruments are essentially high-tech, robotic "measuring sticks" that operate 24/7.
- An explanation of the process you are scheduling in your application, and  
    First, I obtain the difference between the current water level and the Mean Lower Low Water (MLLW), a.k.a. the average lowest level the location reaches, for each location. Then I plot the point on my graph, and then I calculate its relationship with the last two measurements. If it is higher than the last measurement, but lower or the same as the one before that, this is indicative of the tide turning back up from a low point. If it is lower than the last measurement, but higher or equal to the one before that, this indicates the tide turning back down from a high point. If it is selected as a high or low tide, I mark it. 
- A description of your output data and plot.
    My output data, as mentioned above, represents the difference between the current water level and MLLW in four different locations. This is why on my plot, the level rarely goes below 0. The plot oscillates in a sine curve-like pattern, alternating between highs and lows roughly every 6-8 hours as is expected for tides. I attempted to mark the high and low tides with circular points. 