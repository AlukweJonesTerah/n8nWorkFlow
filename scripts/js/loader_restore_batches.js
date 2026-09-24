// "Insert Participants Batch" replaces each item with its own result, and if the
// database errors it collapses to a single error item. So the second database's
// insert re-reads the batches straight from "Parse Window" instead of relying on
// the first insert's output — each database gets every batch of the window, and
// one database failing can't starve the other.
return $('Parse Window').all().map((it) => ({ json: { payload: it.json.payload } }));
