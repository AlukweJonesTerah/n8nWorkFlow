// One row per unmapped column per file (only present on a file's first window).
const events = $input.first().json.events || {};
return (events.unmapped || []).map((u) => ({ json: u }));
