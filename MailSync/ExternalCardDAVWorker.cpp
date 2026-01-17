//
//  ExternalCardDAVWorker.cpp
//  MailSync
//
//  Handles CardDAV sync for external sources with custom credentials
//

#include "ExternalCardDAVWorker.hpp"
#include "MailStore.hpp"
#include "MailStoreTransaction.hpp"
#include "MailUtils.hpp"
#include "Contact.hpp"
#include "ContactGroup.hpp"
#include "NetworkRequestUtils.hpp"
#include "DAVUtils.hpp"
#include "VCard.hpp"

#include <string>
#include <map>
#include <vector>
#include <curl/curl.h>

using namespace std;

typedef string ETAG;

#define EXTERNAL_CARDDAV_SOURCE "external-carddav"

ExternalCardDAVWorker::ExternalCardDAVWorker(
    shared_ptr<Account> account,
    const string& sourceId,
    const string& sourceName,
    const string& sourceUrl,
    const string& sourceUsername,
    const string& sourcePassword
) :
    store(new MailStore()),
    account(account),
    sourceId(sourceId),
    sourceName(sourceName),
    sourceUrl(sourceUrl),
    sourceUsername(sourceUsername),
    sourcePassword(sourcePassword),
    logger(spdlog::get("logger"))
{
    xmlInitParser();
}

ExternalCardDAVSyncResult ExternalCardDAVWorker::run() {
    ExternalCardDAVSyncResult result;
    result.sourceId = sourceId;
    result.sourceName = sourceName;

    logger->info("Starting external CardDAV sync for: {} ({})", sourceName, sourceUrl);

    try {
        // Create or update the ContactBook for this external source
        shared_ptr<ContactBook> ab = resolveAddressBook();
        if (!ab) {
            logger->warn("Could not resolve address book for external source: {}", sourceName);
            result.error = "Could not resolve address book";
            return result;
        }

        // Sync contacts from the address book
        result.contactCount = runForAddressBook(ab);
        result.success = true;

        logger->info("External CardDAV sync completed for: {} ({} contacts)",
                     sourceName, result.contactCount);
    } catch (const exception& e) {
        logger->error("External CardDAV sync failed for {}: {}", sourceName, e.what());
        result.error = e.what();
    }

    return result;
}

shared_ptr<ContactBook> ExternalCardDAVWorker::resolveAddressBook() {
    // Create a unique ID for this external source's contact book
    string bookId = "external-" + sourceId;

    // Check if we already have this contact book
    shared_ptr<ContactBook> existing = store->find<ContactBook>(Query().equal("id", bookId));

    if (!existing) {
        // Create a new ContactBook for this external source
        // Note: We use the account's ID as accountId since contacts need an account reference
        existing = make_shared<ContactBook>(bookId, account->id());
    }

    existing->setSource(EXTERNAL_CARDDAV_SOURCE);
    existing->setURL(sourceUrl);

    // Try to get the ctag from the server to detect changes
    try {
        auto doc = performXMLRequest(
            sourceUrl,
            "PROPFIND",
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<d:propfind xmlns:d=\"DAV:\" xmlns:cs=\"http://calendarserver.org/ns/\">"
            "<d:prop><cs:getctag/><d:displayname/></d:prop>"
            "</d:propfind>",
            "0"
        );

        string ctag = doc->nodeContentAtXPath("//cs:getctag/text()");
        if (ctag != "") {
            existing->setCtag(ctag);
        }
    } catch (const exception& e) {
        logger->warn("Could not fetch ctag for external source {}: {}", sourceName, e.what());
    }

    store->save(existing.get());
    return existing;
}

int ExternalCardDAVWorker::runForAddressBook(shared_ptr<ContactBook> ab) {
    map<ETAG, string> remote {};

    // Fetch all ETags from the remote server
    {
        auto etagsDoc = performXMLRequest(
            ab->url(),
            "REPORT",
            "<c:addressbook-query xmlns:d=\"DAV:\" xmlns:c=\"urn:ietf:params:xml:ns:carddav\">"
            "<d:prop><d:getetag /></d:prop>"
            "</c:addressbook-query>"
        );

        etagsDoc->evaluateXPath("//D:response", ([&](xmlNodePtr node) {
            auto etag = etagsDoc->nodeContentAtXPath(".//D:getetag/text()", node);
            auto href = etagsDoc->nodeContentAtXPath(".//D:href/text()", node);
            remote[string(etag.c_str())] = string(href.c_str());
        }));
    }

    // Get local ETags
    map<ETAG, bool> local {};
    {
        SQLite::Statement findEtags(store->db(), "SELECT etag FROM Contact WHERE bookId = ?");
        findEtags.bind(1, ab->id());
        while (findEtags.executeStep()) {
            local[findEtags.getColumn("etag")] = true;
        }
    }

    // Identify new and deleted contacts
    vector<string> needed {};
    vector<ETAG> deleted {};

    for (auto & pair : remote) {
        if (local.count(pair.first)) continue;
        needed.push_back(pair.second);
    }
    for (auto & pair : local) {
        if (remote.count(pair.first)) continue;
        deleted.push_back(pair.first);
    }

    logger->info("External CardDAV {} - remote: {}, local: {}, needed: {}, deleted: {}",
                 sourceName, remote.size(), local.size(), needed.size(), deleted.size());

    // Fetch needed contacts in chunks
    for (auto chunk : MailUtils::chunksOfVector(needed, 90)) {
        string payload = "";
        for (auto & href : chunk) {
            payload += "<d:href>" + href + "</d:href>";
        }

        auto abDoc = performXMLRequest(
            ab->url(),
            "REPORT",
            "<c:addressbook-multiget xmlns:d=\"DAV:\" xmlns:c=\"urn:ietf:params:xml:ns:carddav\">"
            "<d:prop><d:getetag /><c:address-data /></d:prop>" + payload +
            "</c:addressbook-multiget>"
        );

        MailStoreTransaction transaction {store, "runForExternalAddressBook"};

        // Delete removed contacts
        if (deleted.size()) {
            SQLite::Statement query(store->db(), "DELETE FROM Contact WHERE bookId = ? AND etag = ?");
            for (auto & etag : deleted) {
                query.bind(1, ab->id());
                query.bind(2, etag);
                query.exec();
                query.reset();
            }
            deleted.clear();
        }

        // Save new contacts
        abDoc->evaluateXPath("//D:response", ([&](xmlNodePtr node) {
            bool isGroup = false;
            auto contacts = ingestAddressDataNode(abDoc, node, isGroup);
            for (auto& contact : contacts) {
                contact->setBookId(ab->id());
                if (isGroup) {
                    contact->setHidden(true);
                }
                store->save(contact.get());
            }
        }));

        transaction.commit();
    }

    // Final deletion pass
    if (deleted.size()) {
        SQLite::Statement query(store->db(), "DELETE FROM Contact WHERE bookId = ? AND etag = ?");
        for (auto & etag : deleted) {
            query.bind(1, ab->id());
            query.bind(2, etag);
            query.exec();
            query.reset();
        }
    }

    return remote.size();
}

vector<shared_ptr<Contact>> ExternalCardDAVWorker::ingestAddressDataNode(shared_ptr<DavXML> doc, xmlNodePtr node, bool & isGroup) {
    vector<shared_ptr<Contact>> results;

    auto etag = doc->nodeContentAtXPath(".//D:getetag/text()", node);
    auto href = doc->nodeContentAtXPath(".//D:href/text()", node);
    auto vcardString = doc->nodeContentAtXPath(".//carddav:address-data/text()", node);

    if (vcardString == "") {
        logger->info("Received addressbook entry {} with an empty body", etag);
        return results;
    }

    auto vcard = make_shared<VCard>(vcardString);
    if (vcard->incomplete()) {
        logger->info("Unable to decode vcard: {}", vcardString);
        return results;
    }

    auto emails = vcard->getEmails();
    if (emails.empty()) {
        return results;
    }

    string uid = vcard->getUniqueId()->getValue();
    string baseId = "ext-" + sourceId + "-" + (uid.empty() ? href : uid);

    string name = vcard->getFormattedName()->getValue();
    if (name == "") name = vcard->getName()->getValue();

    // Build contact info with optional photo (shared across all email variants)
    json info = json::object({{"vcf", vcardString}, {"href", href}});

    auto photo = vcard->getPhoto();
    if (photo) {
        string photoValue = photo->getValue();
        if (!photoValue.empty()) {
            info["photo"] = photoValue;
        }
    }

    isGroup = DAVUtils::isGroupCard(vcard);

    // Create a contact for each email address
    for (size_t i = 0; i < emails.size(); i++) {
        string email = emails[i]->getValue();
        if (email.empty()) continue;

        // Create unique ID for each email variant
        string id = (i == 0) ? baseId : baseId + "-" + to_string(i);

        auto contact = store->find<Contact>(Query().equal("id", id));
        if (!contact) {
            contact = make_shared<Contact>(id, account->id(), email, CONTACT_MAX_REFS, EXTERNAL_CARDDAV_SOURCE);
        }

        contact->setInfo(info);
        contact->setName(name);
        contact->setEmail(email);
        contact->setEtag(etag);

        results.push_back(contact);
    }

    return results;
}

const string ExternalCardDAVWorker::getAuthorizationHeader() {
    string plain = sourceUsername + ":" + sourcePassword;
    string encoded = MailUtils::toBase64(plain.c_str(), strlen(plain.c_str()));
    return "Authorization: Basic " + encoded;
}

shared_ptr<DavXML> ExternalCardDAVWorker::performXMLRequest(string _url, string method, string payload, string depth) {
    string url = _url.find("http") != 0 ? "https://" + _url : _url;

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, getAuthorizationHeader().c_str());
    headers = curl_slist_append(headers, "Prefer: return-minimal");
    headers = curl_slist_append(headers, "Content-Type: application/xml; charset=utf-8");

    if (payload.find("urn:ietf:params:xml:ns:carddav") != string::npos) {
        headers = curl_slist_append(headers, "Accept: text/vcard; version=4.0");
    }
    string depthHeader = "Depth: " + depth;
    headers = curl_slist_append(headers, depthHeader.c_str());

    CURL * curl_handle = curl_easy_init();
    const char * payloadChars = payload.c_str();
    curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, 40);
    curl_easy_setopt(curl_handle, CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, payloadChars);

    string result = PerformRequest(curl_handle);
    curl_slist_free_all(headers);
    return make_shared<DavXML>(result, url);
}
