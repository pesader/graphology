from pybliometrics.scopus import ScopusSearch, AbstractRetrieval, init

init()

UNICAMP_AFFILIATION_ID = '60029570'
start_year = 2025

# Query for articles from 2020 onward with authors from the given institution
query = f"AF-ID({UNICAMP_AFFILIATION_ID}) AND PUBYEAR > {start_year - 1}"

# Fetch results with pagination
search = ScopusSearch(query, subscriber=True)

# Iterate over each article
for result in search.results:
    print(result)

    # Document information
    title = result.title
    eid = result.eid
    doi = result.doi
    openaccess = result.openaccess
    date = result.coverDate
    document_type = result.subtype
    document_type_description = result.subtypeDescription
    first_author = result.creator

    # Funding
    funding_acronym = result.fund_acr
    funding_number = result.fund_no
    funding_name = result.fund_sponsor

    # Source (Journal, Magazine, etc)
    source_name = result.publicationName
    source_type = result.aggregationType
    source_id = result.source_id
    source_issn = result.issn
    source_eissn = result.eIssn

    # Volume, issue, pages
    volume = result.volume
    issue = result.issueIdentifier
    page = result.pageRange

    # Citations count
    citedby_count = result.citedby_count

    # Retrieve full article metadata
    abstract = AbstractRetrieval(eid, view="FULL")
    language = abstract.language
    areas = abstract.subject_areas

    # Conference
    conference_code = abstract.confcode
    conference_date = abstract.confdate
    conference_location = abstract.conflocation
    conference_name = abstract.confname
    conference_sponsors = abstract.confsponsor

    # Redundancy
    page_start = abstract.startingPage
    page_end = abstract.endingPage

    # Keywords
    author_keywords = result.authkeywords
    indexed_keywords = abstract.idxterms

    # Authors
    for author in abstract.authorgroup:
        author_affiliation_id = author.affiliation_id
        author_collaboration_id = author.collaboration_id
        author_department_id = author.dptid
        author_organization = author.organization
        author_id = author.auid
        author_name = author.indexed_name
        author_given_name = author.given_name
        author_surname = author.surname
        author_country = author.country
        print(f"Author: Name={author_name} ID={author_id} Affiliation={author_affiliation_id} Department={author_department_id} Country={author_country}")

    # Collaborators
    if abstract.contributor_group:
        for contributor in abstract.contributor_group:
            contributor_name= contributor.indexed_name
            contributor_given_name= contributor.given_name
            contributor_surname= contributor.surname
            contributor_role = contributor.role
            print(f"Contributor: Name={contributor_name} Role={contributor_role}")

    print('-' * 40)

# Entities
#   - Document
#   - Author
#   - Source
#   - Conference
#   - Funding agency
