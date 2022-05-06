# Real-Time-Big-Data

In this project, we try to find the influence of economic status on social media like Twitter, Instagram, in each state of the United States. 
We collect the posts data from several social media platforms such as Twitter, Instagram, and some check-in platforms over a period of time, we then combine these posts data with the economic data of each state over that time period. For each social media, we analyzed the relationship between the number of posts in each state in a month and the state's economic situation for that month.

The dataset only contains latitude and longitude data instead of state data, so in data profiling, I have to get the where the check-in happens given the latitude and longitude. I first plan to make a request per data row through Google reverse geocoding. However, this method not only costs too much with millions of requests, but also is time consuming. I then use a rectangle to wrap around the United States mainlands use the reverse geocoding API to build a matrix that maps the latitude and longitude to the name of the state. Then we can use this matrix to replace the Google reverse geocoding API in Map-Reduce job which largely accelerates the process.

The build_matrix directory contains a python code that use PyGeo to build the matrix that maps the latitude and longitude to the state name of the United States.

The DataProfiling directory contains some Map-Reduce codes that I use to profile and clean the data.

For the detailed description of what we have done, please refer to the paper attached.
