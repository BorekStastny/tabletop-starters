# tabletop-starters

Starter decks for Tabletop, refreshed daily: draft decks that went 7 wins and at most 2 losses in the newest set's
Premier Draft queue, from players with a good win rate, two colours each.

- `index.json` lists today's decks (`id`, `name`, `colors`, `record`, `file`).
- `decks/<SET>/<id>.txt` is one deck each, in the plain list format (`About` / `Name` / `Deck` / `4 Card Name`).
- `scripts/build-starters.mjs` builds both from 17Lands' public game data; `.github/workflows/refresh.yml` runs it every day.

```
node scripts/build-starters.mjs                 # newest set that 17Lands has data for
node scripts/build-starters.mjs --set FRA       # a given set
```

## Credits

Deck data comes from the [17Lands public datasets](https://www.17lands.com/public_datasets) (CC BY 4.0).
Card names are Wizards of the Coast's. This is unofficial fan content, not approved or endorsed by Wizards of the Coast.
