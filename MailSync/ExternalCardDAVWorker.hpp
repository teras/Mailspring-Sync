//
//  ExternalCardDAVWorker.hpp
//  MailSync
//
//  Handles CardDAV sync for external sources with custom credentials
//

#ifndef ExternalCardDAVWorker_hpp
#define ExternalCardDAVWorker_hpp

#include "Account.hpp"
#include "ContactBook.hpp"
#include "Contact.hpp"
#include "MailStore.hpp"
#include "DavXML.hpp"

#include <string>
#include <memory>

#include "spdlog/spdlog.h"

using namespace std;

struct ExternalCardDAVSyncResult {
    string sourceId;
    string sourceName;
    int contactCount = 0;  // Total contacts after sync
    bool success = false;
    string error;
};

class ExternalCardDAVWorker {
    MailStore * store;
    shared_ptr<spdlog::logger> logger;

    // External source configuration
    string sourceId;
    string sourceName;
    string sourceUrl;
    string sourceUsername;
    string sourcePassword;

    // Account used for storing contacts (they need an accountId)
    shared_ptr<Account> account;

    // Cache for address book discovery
    shared_ptr<ContactBook> cachedAddressBook = nullptr;

public:
    ExternalCardDAVWorker(
        shared_ptr<Account> account,
        const string& sourceId,
        const string& sourceName,
        const string& sourceUrl,
        const string& sourceUsername,
        const string& sourcePassword
    );

    ExternalCardDAVSyncResult run();

private:
    shared_ptr<ContactBook> resolveAddressBook();
    int runForAddressBook(shared_ptr<ContactBook> ab);  // returns total contact count

    vector<shared_ptr<Contact>> ingestAddressDataNode(shared_ptr<DavXML> doc, xmlNodePtr node, bool & isGroup);

    const string getAuthorizationHeader();
    shared_ptr<DavXML> performXMLRequest(string path, string method, string payload = "", string depth = "1");
};

#endif /* ExternalCardDAVWorker_hpp */
