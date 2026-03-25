# TODO Tracking Preferences

## Structure

```
TODO/
├── TODO.md              # Master index — links to topic files, sections below
├── {topic}.md           # One file per major workstream (e.g., gas-ebb-dashboard.md)
└── daily_logs/
    └── today.md         # Daily working list, reset or carry forward each morning
```

## Master TODO.md Sections

1. **Current Focus** — active workstream with link to topic file
2. **Next Up** — queued tasks
3. **Backlog** — lower-priority items
4. **Blocked** — items waiting on external input
5. **Done** — completed items (`[x] DONE`)
6. **Open Questions** — tagged `QUESTION`
7. **Priority Recommendations** — ranked table with `#`, `Task`, `Why`

## Tags

`TODO` | `DOING` | `BLOCKED` | `DONE` | `QUESTION` — picked up by Todo Tree.

## Conventions

- Checkboxes: `- [ ] TODO` for open, `- [x] DONE` for complete
- Topic files get their own `## Open Questions` section at the bottom
- Daily log (`today.md`) uses the current date as the `#` heading
- When adding bugs, prefix with `BUG` tag: `- [ ] BUG description — discovered context`
- Keep master TODO.md concise — detail lives in topic files
