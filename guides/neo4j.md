### Before you start
- You need a **Connection URI**, a **username** and a **password** for both the old and the new Neo4j.
- The new database should be **empty**. The tool refuses to load into a database that already has data, so nothing gets duplicated.
- **AuraDB Free** pauses when nobody uses it. Open **console.neo4j.io** and click **Resume** on both instances before you start.

---

### Where to find each value

**Connection URI**
- **AuraDB** (Neo4j's cloud): open **console.neo4j.io** → your instance → the **Connection URI** on the instance card. It looks like `neo4j+s://xxxxxxxx.databases.neo4j.io`.
- **Neo4j Desktop**: open the database → **Connect** / details panel → **Bolt port**. Usually `bolt://localhost:7687`.
- **Your own server**: `neo4j://<server-name>:7687` (add `+s` if it uses a trusted certificate, or `+ssc` for a self-signed one).

**Username and password**
- **AuraDB**: when you created the instance, a **credentials file** (`.txt`) was downloaded. It contains `NEO4J_USERNAME` (usually `neo4j`) and `NEO4J_PASSWORD`. Lost it? On the instance card use **⋯ → Reset password**.
- **Desktop / server**: the user and password you set when creating the database.

**Advanced (optional)**
- **Only these databases** (old account): leave empty to copy every database except `system`.
- **Database to restore into** (new account): leave empty to keep the same names. **AuraDB Free and Community Edition have only one database**, so enter `neo4j` here.
- **Rows per batch**: lower it (for example `1000`) if a small instance runs out of memory.

---

### What gets copied
- Every node with its labels and properties, and every relationship with its type and properties.
- Constraints and indexes, including full-text and vector indexes.
- All property types: numbers, text, true/false, lists, dates, times, durations, points (coordinates) and bytes.
- **Check** compares counts per label and relationship type, plus a fingerprint of all data, so a green result means the graphs are identical.

### Your saved queries (Aura Query tool)
Saved Cypher, your open query tabs and your query history are kept **with your Aura login**, not inside the database, so the tool collects them separately:

1. On **2. Back up**, keep **Also collect my saved queries from Aura** ticked and press **Back up now**.
2. A browser window opens (Edge or Chrome). **Sign in to Aura with the old account**, the same way you normally do.
3. The app opens the Query tool by itself, reads your queries and closes the window. Then the database backup runs.
   - If it says "No queries yet": in that window, connect to your instance if asked, open the **Saved Cypher** panel (bookmark icon), then close the window.
4. The backup report lists how many queries were saved. They're also stored as readable `.cypher` files in the backup folder (`neo4j_queries`).

After the restore, the **Restore** page shows **Your saved queries**, each with a copy button, plus a button that opens the new Aura Query tool. Neo4j doesn't allow outside tools to add entries to the new account's Saved Cypher list, so this last part is paste-and-bookmark.

Only the queries are kept. The tool doesn't save your Aura password, cookies or anything else from the website, and the browser window uses its own separate profile.

### What needs you afterwards
- **Users and custom roles** (Enterprise / AuraDB Business Critical): passwords can't be read. The app gives the exact `CREATE ROLE`, `GRANT` and `CREATE USER` commands.
- **Plugins** like APOC or Graph Data Science, and things that live in them (APOC triggers, stored GDS models), must be installed or recreated in the new Neo4j.

---

### Common errors

| Message contains | Fix |
|---|---|
| `Unauthorized` / `authentication failure` | Wrong password. AuraDB: use the credentials file, or reset the password |
| `Unable to retrieve routing information` / `ServiceUnavailable` | Wrong URI, or the AuraDB instance is paused. Resume it in the console |
| `certificate` / `SSL` | Use `neo4j+s://` for AuraDB, `neo4j+ssc://` for a self-signed server |
| `Unsupported administration command` | This Neo4j has only one database. Set 'Database to restore into' to `neo4j` |
| `the target database isn't empty` | Empty it (the app gives the command) or choose another database |
| `MemoryPool` / `timeout` | Lower 'Rows per batch' |
