# Refaded
## A reimplmentation of Crossfadded/[hiphop_web](https://github.com/nlipsyc/hiphop_web)

This is a second pass at Crossfaded, a project that identifies and makes explorable instances where one Hip Hop artist references the lyrics of another.

This project uses the Kaggle/Genius corpus to extract ngrams from Hip Hop songs, finds ones that occur across multiple artists, and then loads them into a graph DB for exploration.

Once this is completed there will be a website created to allow browsing of links between artists including path searches and high frequency references