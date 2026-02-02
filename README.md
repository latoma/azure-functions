# PDF Translation Function App

Azure Functions app that automatically translates PDF documents when uploaded to blob storage. Uses Event Grid blob trigger with Durable Functions orchestration.

## Architecture

```
Upload PDF → Blob Storage (unprocessed-pdf)
     ↓
Event Grid System Topic
     ↓
Function App (blob trigger)
     ↓
Durable Orchestrator → Document Translation API
     ↓
Output → Blob Storage (processed-pdf/{lang}_filename.pdf)
```

## Prerequisites

- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli)
- [Azure Functions Core Tools v4](https://learn.microsoft.com/azure/azure-functions/functions-run-local#install-the-azure-functions-core-tools)
- [Python 3.12+](https://www.python.org/downloads/)
- An existing Azure resource group with:
  - Storage account
  - Document Translation service (with endpoint and API key)

## Quick Start

### 1. Configure Parameters

Edit [infra/parameters.json](infra/parameters.json) with your values:

```json
{
  "parameters": {
    "environmentName": { "value": "your-env-name" },
    "storageAccountName": { "value": "your-existing-storage-account" },
    "enableMonitoring": { "value": true },
    "developerPrincipalId": { "value": "" }
  }
}
```

### 2. Deploy

