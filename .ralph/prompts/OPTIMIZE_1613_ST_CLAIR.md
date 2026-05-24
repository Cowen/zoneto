We are going to compare our evaluations to an a real Council-approved application at 1613 ST CLAIR. Add relevant data, BERT-based evaluators, and LLM prompt tweaks until we have optimized and converged on the highest possible confidence score.

For any new data you give to our embedding rules or the LLM narrator, make sure that its signal is highly differentiable and not an artifact of survivorship bias or incomplete data. We have already learned the open data sets that we are using are not the most robust.

You have: 
* The open data portal listing for 1613 St Clair Avenue West is here: https://www.toronto.ca/city-government/planning-development/application-details/?id=5124543&pid=170581&title=1613%20ST%20CLAIR%20AVE%20W
* However there are multiple folder RSNs for this address. You can find all of them in our dataset.
* All of the other zoning and geospatial information within our dataset

And the description given is:

"The developer intends to develop the subject lands with a 17-storey, 57 meter mixed-use building consisting of 258 units in 16,732 square meters of residential floor area and 1,404 square meters of non-residential floor area. Non-residential floor area would be comprised of 57square metres of community space, 271 square metres of retail, and 1,076 square metres of medical office. A total of 575.4 square meters of indoor amenity area and 721 square meters of outdoor amenity area is proposed. A Site Plan Control application has also been submitted in support of the proposal and has been circulated concurrently. The proposal has been revised to provide medical office space and community space noted above, simplified floor plates, relocation of parking to provide nine vehicle parking spaces above grade and 92 parking spaces below grade."

Because this proposal was ultimately approved in real life, we should be able to score this proposal as having high confidence.

However, we should score this same proposal very poorly when applied to the address 321 Boon Avenue, which has many significant violations as determined by our BERT-based rule scorers.
