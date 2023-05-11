const hubspot = require("@hubspot/api-client");
const { queue: asyncQueue } = require("async");
const _ = require("lodash");

const { filterNullValuesFromObject, goal } = require("./utils");
const Domain = require("./Domain");

const hubspotClient = new hubspot.Client({ accessToken: "" });
const propertyPrefix = "hubspot__";
let expirationDate;
const RETRIALS = 2;

const manageAsyncOps = async (fn) => {
    try {
        const response = await fn;
        return [null, response];
    } catch (error) {
        return [error, null];
    }
};

const queries = {
    contacts: (searchObject) => hubspotClient.crm.contacts.searchApi.doSearch(searchObject),
    companies: (searchObject) => hubspotClient.crm.companies.searchApi.doSearch(searchObject),
    meetings: (searchObject) => hubspotClient.crm.objects.meetings.searchApi.doSearch(searchObject),
};

const generateLastModifiedDateFilter = (date, nowDate, propertyName = "hs_lastmodifieddate") => {
    const lastModifiedDateFilter = date
        ? {
              filters: [
                  { propertyName, operator: "GTE", value: `${date.valueOf()}` },
                  { propertyName, operator: "LTE", value: `${nowDate.valueOf()}` },
              ],
          }
        : {};

    return lastModifiedDateFilter;
};

const saveDomain = async (domain) => {
    // disable this for testing purposes
    return;

    domain.markModified("integrations.hubspot.accounts");
    await domain.save();
};

/**
 * This function allows for retrieving data from hubspot
 * This function uses async-await and improved cleaner error checks and error handling thus making async code look synchronous
 * **/
const pullDataFromHubspot = async () => {
    const domain = await Domain.findOne({});

    if (!domain) throw new Error("Failed to retrieve data from hubspot as domain information could not be retrieved");

    if (domain?.integrations?.hubspot?.accounts && domain.integrations.hubspot.accounts.length == 0)
        throw new Error("There are no associated domain accounts to pull data from");

    for (const account of domain.integrations.hubspot.accounts) {
        const [tokenError] = await manageAsyncOps(_generateRefreshTokens(domain, account));
        if (tokenError)
            throw new Error(
                JSON.stringify({
                    error: tokenError.message,
                    apiKey: domain.apiKey,
                    metadata: { operation: "refreshAccessToken" },
                })
            );

        const queue = await _createAQueue(domain, []);

        const [contactsError] = await manageAsyncOps(_processContacts(domain, account, queue));
        if (contactsError)
            throw new Error(
                JSON.stringify({
                    error: contactsError.message,
                    apiKey: domain.apiKey,
                    metadata: { operation: "processContacts" },
                })
            );

        const [companiesError] = await manageAsyncOps(_processCompanies(domain, account, queue));

        if (companiesError)
            throw new Error(
                JSON.stringify({
                    error: companiesError?.message,
                    apiKey: domain.apiKey,
                    metadata: { operation: "processCompanies" },
                })
            );

        const [meetingsError] = await manageAsyncOps(_processMeetings(domain, account, queue));

        if (meetingsError) {
            console.log("meetings: ", meetingsError);
            throw new Error(
                JSON.stringify({
                    error: meetingsError?.message,
                    apiKey: domain.apiKey,
                    metadata: { operation: "processMeetings" },
                })
            );
        }

        const [drainError] = await manageAsyncOps(queue.drain());
        console.log(drainError);
        console.log("finish processing account");
        saveDomain(domain);
    }
};

/**
 * This private uses async-await instead of promises, providing a simpler easy to read interface to generate hubspot tokens
 * **/
const _generateRefreshTokens = async (domain, account) => {
    const hubspotClientId = process.env.HUBSPOT_CID;
    const hubspotClientSecret = process.env.HUBSPOT_CS;

    const tokenResponse = await hubspotClient.oauth.tokensApi.createToken(
        "refresh_token",
        undefined,
        undefined,
        hubspotClientId,
        hubspotClientSecret,
        account.refreshToken
    );

    hubspotClient.setAccessToken(tokenResponse.accessToken);

    expirationDate = new Date(tokenResponse.expiresIn * 1000 + new Date().getTime());

    if (account.accessToken != tokenResponse.accessToken) {
        account.accessToken = tokenResponse.accessToken;
        domain.save();
    }
};

/**
 * This private function uses modern async-await for create async-queue
 * **/
const _createAQueue = async (domain, actions) => {
    const queue = asyncQueue(async (action) => {
        actions.push(action);

        if (actions.length > 2000) {
            console.log("inserting actions to database", { apiKey: domain.apiKey, count: actions.length });

            const copyOfActions = _.cloneDeep(actions);
            actions.splice(0, actions.length);

            goal(copyOfActions);
        }
    }, 100000000);

    return {
        push: async (action) => {
            await queue.push(action);
        },
        length: () => {
            return queue.length();
        },
        drain: async () => {
            if (queue.length() > 0) await queue.drain();

            if (actions.length > 0) {
                goal(actions);
            }

            return true;
        },
    };
};

/**
 * Get recently modified companies as 100 companies per page
 */
const _processCompanies = async (domain, account, queue) => {
    // We pass the account object to prevent unnecessary find operation
    // const account = domain.integrations.hubspot.accounts.find((account) => account.hubId === hubId);
    const lastPulledDate = new Date(account?.lastPulledDates?.companies);
    const now = new Date();

    let hasMore = true;
    const offsetObject = {
        after: undefined,
        lastModifiedDate: undefined,
    };
    const limit = 100;

    while (hasMore) {
        const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
        const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
        const searchObject = {
            filterGroups: [lastModifiedDateFilter],
            sorts: [{ propertyName: "hs_lastmodifieddate", direction: "ASCENDING" }],
            properties: ["name", "domain", "country", "industry", "description", "annualrevenue", "numberofemployees", "hs_lead_status"],
            limit,
            after: offsetObject.after,
        };

        const [error, searchResult] = await manageAsyncOps(_recursivelyRetrieveEntities(searchObject, "companies"));

        if (error || !searchResult?.results) throw new Error("Failed to retrieve companies");

        const data = searchResult.results || [];

        offsetObject.after = parseInt(searchResult?.paging?.next?.after);

        console.log("fetch company batch");

        data.forEach((company) => {
            if (!company.properties) return;

            const actionTemplate = {
                includeInAnalytics: 0,
                companyProperties: {
                    company_id: company.id,
                    company_domain: company.properties.domain,
                    company_industry: company.properties.industry,
                },
            };

            const isCreated = !lastPulledDate || new Date(company.createdAt) > lastPulledDate;

            queue.push({
                actionName: isCreated ? "Company Created" : "Company Updated",
                actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
                ...actionTemplate,
            });
        });

        if (!offsetObject?.after) {
            hasMore = false;
            break;
        } else if (offsetObject?.after >= 9900) {
            offsetObject.after = 0;
            offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
        }
    }

    account.lastPulledDates.companies = now;
    await saveDomain(domain);

    return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const _processContacts = async (domain, account, queue) => {
    const lastPulledDate = new Date(account.lastPulledDates.contacts);
    const now = new Date();

    let hasMore = true;
    const offsetObject = {};
    const limit = 100;

    while (hasMore) {
        const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
        const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, "lastmodifieddate");
        const searchObject = {
            filterGroups: [lastModifiedDateFilter],
            sorts: [{ propertyName: "lastmodifieddate", direction: "ASCENDING" }],
            properties: [
                "firstname",
                "lastname",
                "jobtitle",
                "email",
                "hubspotscore",
                "hs_lead_status",
                "hs_analytics_source",
                "hs_latest_source",
            ],
            limit,
            after: offsetObject.after,
        };

        const [error, searchResult] = await manageAsyncOps(_recursivelyRetrieveEntities(searchObject, "contacts"));

        if (error || !searchResult?.results) throw new Error("Failed to retrieve contacts");

        const data = searchResult.results || [];

        offsetObject.after = parseInt(searchResult.paging?.next?.after);
        console.log("fetch contact batch");
        const contactIds = data.map((contact) => contact.id);

        // contact to company association
        const contactsToAssociate = contactIds;
        const companyAssociationsResults =
            (
                await (
                    await hubspotClient.apiRequest({
                        method: "post",
                        path: "/crm/v3/associations/CONTACTS/COMPANIES/batch/read",
                        body: { inputs: contactsToAssociate.map((contactId) => ({ id: contactId })) },
                    })
                ).json()
            )?.results || [];

        const companyAssociations = Object.fromEntries(
            companyAssociationsResults
                .map((a) => {
                    if (a.from) {
                        contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
                        return [a.from.id, a.to[0].id];
                    } else return false;
                })
                .filter((x) => x)
        );

        data.forEach((contact) => {
            if (!contact.properties || !contact.properties.email) return;

            const companyId = companyAssociations[contact.id];

            const isCreated = new Date(contact.createdAt) > lastPulledDate;

            const userProperties = {
                company_id: companyId,
                contact_name: ((contact.properties.firstname || "") + " " + (contact.properties.lastname || "")).trim(),
                contact_title: contact.properties.jobtitle,
                contact_source: contact.properties.hs_analytics_source,
                contact_status: contact.properties.hs_lead_status,
                contact_score: parseInt(contact.properties.hubspotscore) || 0,
            };

            const actionTemplate = {
                includeInAnalytics: 0,
                identity: contact.properties.email,
                userProperties: filterNullValuesFromObject(userProperties),
            };

            queue.push({
                actionName: isCreated ? "Contact Created" : "Contact Updated",
                actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
                ...actionTemplate,
            });
        });

        if (!offsetObject?.after) {
            hasMore = false;
            break;
        } else if (offsetObject?.after >= 9900) {
            offsetObject.after = 0;
            offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
        }
    }

    account.lastPulledDates.contacts = now;
    await saveDomain(domain);

    return true;
};

const _processMeetings = async (domain, account, queue) => {
    const now = new Date();

    const offsetObject = {
        after: undefined,
        lastModifiedDate: undefined,
    };
    const limit = 100;

    const searchObject = {
        filterGroups: {
            filters: [
                {
                    propertyName: "lastUpdated",
                    operator: "LTE",
                    value: now.valueOf(),
                },
            ],
        },
        sorts: [{ propertyName: "lastUpdated", direction: "ASCENDING" }],
        properties: ["hs_meeting_title", "hs_timestamp"],
        limit,
        after: offsetObject.after,
    };

    const [error, responseData] = await manageAsyncOps(_recursivelyRetrieveEntities(searchObject, "meetings"));

    if (error || !responseData?.results) throw new Error("Failed to retrieve meetings");

    const data = responseData.results || [];
    console.log("fetch meeting batch");

    offsetObject.after = parseInt(responseData.paging?.next?.after);

    for (const meeting of responseData?.results) {
        const actionTemplate = {
            includeInAnalytics: 0,
            meetingProperties: {
                meeting_title: meeting.properties.hs_meeting_title,
                meeting_timestamp: meeting.properties.hs_timestamp,
            },
        };

        queue.push({
            actionName: "Meeting Created",
            actionDate: new Date(meeting.createdAt) - 2000,
            identity: meeting.id,
            ...actionTemplate,
        });
    }

    offsetObject.after = 0;
    offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();

    account.lastPulledDates.meetings = now;
    await saveDomain(domain);
    return true;
};

/**
 * This is a private method that aims to via recursion retrieve hubspot entities e.g companies, contacts and meetings
 * The idea is to provide a singular function that provides a common interface for entity retrieval
 **/
const _recursivelyRetrieveEntities = async (searchObject, entity, tryCount = 0) => {
    const [error, searchResult] = await manageAsyncOps(queries?.[entity](searchObject));

    if (error || !searchObject) {
        tryCount++;
        if (tryCount >= RETRIALS) {
            throw new Error(`Failed to fetch ${entity} after ${RETRIALS} tries. Aborting.`);
        }

        if (new Date() > expirationDate) {
            return "TOKEN_EXPIRED";
        }

        const delay = 5000 * Math.pow(2, tryCount);
        console.log(`Error fetching ${entity}. Retrying in ${delay} ms.`);
        await new Promise((resolve) => setTimeout(resolve, delay));
        return _recursivelyRetrieveEntities(searchObject, entity, tryCount);
    }
    return searchResult;
};

module.exports = pullDataFromHubspot;
