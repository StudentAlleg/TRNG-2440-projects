I analyzed the following cities:
* San Jose, CA
* Martinez, CA
* Davis, CA
* Roseville, CA

From May 1st, 2026 to June 1st, 2026.
These cities have a special meaning to me, they are all origins/destinations that I have regularly used on the intercity train the Capitol Corridor. I originally put in the coordinates of the stations, which led to the most significant data issues.
When the API returns the lat/long for the information requested, it does not return the lat/long you entered, it returns the lat/long of the node it actually used. This led to a discrepancy in my assumptions. I was using the lat/long returned by the API call to match up with the named locations. As a part of the cleaning process, I rematched the data so that I could have a nice name and an easier insert into the database. I rejected data that did not have a matching location. Because of this, I was not actually getting any data from my cleaning stage. The fix was to update my locations lat/long with the lat/long



data issues: need to split tables better, cannot just do read_json. Needed to fix locations. Location lat/long returned was not the same as inputted (bad assumption). First tried rounding, but on double checking the data it was not the same as input (using it to match locatoin with name)