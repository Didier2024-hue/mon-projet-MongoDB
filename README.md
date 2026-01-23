📚 MongoDB – Books Data Exploration & Analytics (Evaluation Project)

DataScientest | Data Engineering / Data Analytics

🎯 Business Context

The project simulates a data exploration mission for a publishing platform, where the objective is to extract actionable insights from a book catalog. The dataset contains books related to programming and data science, and the business goal is to understand:

which books are most relevant and popular,

the distribution of categories and authors,

content quality and publication trends,

keyword-based insights for marketing and catalog strategy.

🧱 Data Architecture & Approach

The project follows a simple but scalable data architecture:

1️⃣ Raw Data Layer

A JSON dataset is imported into MongoDB as the books collection.

This layer represents the source of truth for all book metadata.

2️⃣ Analytical Layer (Aggregation Pipelines)

Business logic is implemented via MongoDB aggregation pipelines.

Each question is resolved with a single pipeline to ensure performance and scalability.

Pipelines include operations such as:

filtering ($match)

array manipulation ($unwind, $arrayElemAt)

grouping ($group)

projection ($project)

sorting & limiting ($sort, $limit)

3️⃣ Reporting Layer

Results are used to answer business questions like:

which authors publish most frequently,

which categories contain the longest books,

what programming languages are most discussed,

publication trends over time.

📌 Key Deliverables
Data Setup

Import JSON dataset into MongoDB (sample.books)

Analytical Outputs (via PyMongo)

The evaluation consists of a series of business-oriented queries executed using PyMongo:

Book counts (page count, publication status)

Keyword-based searches (e.g., “Android”)

Category hierarchy extraction

Statistical metrics per category (min, max, avg pages)

Publication date analysis (year/month/day)

Author analysis (top authors, distribution of author counts)

Content insights by language keywords
