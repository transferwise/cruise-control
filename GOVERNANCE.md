# Governance

This document defines project governance for the Cruise Control for Kafka project.

Technical Steering Committee
=============================

The Technical Steering Committee (TSC) is responsible for technical oversight of the project.

TSC Chair: **Viktor Somogyi-Vass** - @viktorsomogyi
TSC Co-Chair: **Kyle Liberti** - @kyguy

### TSC members:

All TSC members are Maintainers and voting members.

  - **Adem Efe Gencer** - @efeg
  - **Hao Geng** - @CCisGG
  - **Nick Garvey** - @nickgarvey
  - **Maryan Hratson** - @mhratson
  - **Allen Wang** - @allenxwang
  - **Tamas Barnabas Egyed** - @egyedt / @egytom
  - **Chia-Ping Tsai** - @chia7712
  - **Krit Petty** - @bgrishinko
  - **Jiangjie (Becket) Qin** - @becketqin
  - **Viktor Somogyi-Vass** - @viktorsomogyi
  - **Omkhar Arasaratnam** - @omkhar
  - **Bertalan Kondrat** - @k0b3rIT
  - **Paolo Patierno** - @ppatierno
  - **Mickael Maison** - @mimaison
  - **Kyle Liberti** - @kyguy

### Emeritus members

TSC members who are no longer actively participating in project governance may move to Emeritus status.
Emeritus is an honorary recognition and is not a TSC role.
Emeritus members are not counted as part of the TSC for voting or quorum purposes.
A TSC member may voluntarily move to Emeritus status at any time.

The TSC will conduct a yearly review of member activity. 
Members who have not been actively participating will be contacted and asked about their continued involvement.
Members who wish to step back will be moved to Emeritus status.
An Emeritus member may return to active TSC membership through the same process as adding a new TSC member.

### Project Roles

- **Contributor**: Anyone who contributes code, documentation, or other technical artifacts
  to the project (See [Charter, Section 2.c.i](CHARTER.md)).

- **Maintainer**: A Contributor who has earned the ability to commit to the project's repository and participate in standard
  and consensus voting (See Voting Process below).
  A Contributor may become a Maintainer by a majority approval of the TSC.
  A Maintainer may be removed by a majority approval of the TSC (See [Charter, Section 2.c.ii-iii](CHARTER.md)).

- **TSC Member**: A Maintainer who participates in project governance and all voting (See Voting Process below).
  At this time, all TSC members are also Maintainers.
  A Maintainer may become a TSC member by a majority approval of the TSC (See Charter, Section 2.c.iii).
  Nominations may be made by any existing TSC member.

### Voting Process

The TSC aims to operate as a consensus-based community (See [Charter, Section 3.a](CHARTER.md)).
Most day-to-day project decisions are made through a standard vote.
Interface and complex changes require a consensus vote.
Governance changes, or decisions where consensus cannot be reached, require a majority vote.

**Standard Vote**: Requires at least **2 maintainer approvals**.
If the author of the change is a maintainer, that counts as one approval.

**Consensus Vote**: Requires active approval within the voting period.

  - Must remain open for at least **3 days** (72 hours)
  - Requires at least **3 binding +1 votes** and **zero binding -1 votes**
  - A -1 (objection) must include a technical justification; an objection without justification is not binding

Any maintainer may request that a consensus vote be escalated to a majority vote by the TSC.

**Majority Vote**: Decided by a simple majority of a quorum of TSC members.

- **At a meeting**: A majority of those in attendance, provided quorum (50% of all voting TSC members) is met.
- **By electronic vote**: A majority of all voting members of the TSC.

Formal votes must remain open for at least **7 days** (168 hours).

Charter amendments require a two-thirds vote of the entire TSC and are subject
to approval by LF Projects (See [Charter, Section 8.a](CHARTER.md)).