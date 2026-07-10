I analyzed the following cities:
* San Jose, CA
* Martinez, CA
* Davis, CA
* Roseville, CA

From May 1st, 2026 to June 1st, 2026.
These cities have a special meaning to me, they are all origins/destinations that I have regularly used on the intercity train the Capitol Corridor. I originally put in the coordinates of the stations, which led to the most significant data issues.

When the API returns the lat/long for the information requested, it does not return the lat/long you entered, it returns the lat/long of the node it actually used. This led to a discrepancy in my assumptions. I was using the lat/long returned by the API call to match up with the named locations. As a part of the cleaning process, I rematched the data so that I could have a nice name and an easier insert into the database. I rejected data that did not have a matching location. Because of this, I was not actually getting any data from my cleaning stage. The fix was to update my locations lat/long with the lat/long returned by the API, so I could correctly assign usable names.

Another issue I needed to work through was reading the json request. Due to the format, we would need to change how it would work, either by flattening the json object or doing some different way. I chose to convert it to a pydantic model with the full paramters, making it very easy to retrieve the data that I wanted.
