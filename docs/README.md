# Lance-Ray Documentation

This directory contains the documentation for Lance-Ray, built with MkDocs.

## Building Documentation

### Prerequisites

Install uv and the documentation dependencies:

```bash
# From the project root
uv pip install -e ".[docs]"
```

### Local Development

To serve the documentation locally with hot-reload:

```bash
cd docs
uv run mkdocs serve
```

The documentation will be available at http://localhost:8000

### Building Static Files

To build the static documentation:

```bash
cd docs
uv run mkdocs build
```

The built documentation will be in the `site/` directory.

## Documentation Structure

- `mkdocs.yml` - MkDocs configuration
- `src/` - Documentation source files in Markdown
  - `index.md` - Homepage
  - `read.md` - Read operations guide
  - `write.md` - Write operations guide  
  - `distributed-indexing.md` - Distributed indexing guide
  - `compaction.md` - Distributed compaction guide
  - `cleanup.md` - Distributed cleanup guide
  - `data-evolution.md` - Data evolution guide
  - `examples.md` - Usage examples
  - `.pages` - Navigation configuration

## Adding New Pages

1. Create a new `.md` file in `src/`
2. Update `src/.pages` to add it to navigation
3. Follow the existing documentation style

## Deployment

Documentation is automatically deployed to GitHub Pages when changes are pushed to the main branch.