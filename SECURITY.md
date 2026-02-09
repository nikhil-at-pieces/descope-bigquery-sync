# Security Configuration

This document explains the security measures implemented for the Descope BigQuery Sync Cloud Function.

## Security Features

### 1. 🔐 Google Secret Manager Integration
**Problem**: Storing sensitive credentials in environment variables or code makes them visible in deployment configs and logs.

**Solution**: All sensitive credentials are stored in Google Secret Manager:
- `descope-project-id` - Descope project identifier
- `descope-management-key` - Descope API management key

**Benefits**:
- Secrets are encrypted at rest
- Access is audited and logged
- Secrets can be rotated without redeploying
- Secrets never appear in environment variables or logs

### 2. 🔒 Private Cloud Function (No Public Access)
**Problem**: `--allow-unauthenticated` flag allows anyone with the URL to trigger the function.

**Solution**: Function is deployed with `--no-allow-unauthenticated`:
- Only authenticated requests are allowed
- IAM policies control who can invoke the function
- Public access is completely blocked

### 3. 🎫 OIDC Authentication for Cloud Scheduler
**Problem**: Without authentication, anyone could impersonate Cloud Scheduler.

**Solution**: Cloud Scheduler uses OIDC (OpenID Connect) tokens:
- Scheduler uses a dedicated service account
- OIDC tokens are automatically generated and verified
- Each request includes a cryptographically signed token
- Function verifies the token authenticity

### 4. 🛡️ Request Validation
**Problem**: Even with IAM, additional validation helps prevent unauthorized access.

**Solution**: Function validates incoming requests:
- Checks for Cloud Scheduler User-Agent header
- Validates Authorization header presence
- Returns 403 Forbidden for suspicious requests
- Logs all unauthorized access attempts

### 5. 🔑 Least Privilege Service Accounts
**Problem**: Using default service accounts grants excessive permissions.

**Solution**: Dedicated service accounts with minimal permissions:

**Function Service Account** (`descope-sync-sa`):
- `roles/bigquery.dataEditor` - Only what's needed for BigQuery operations
- `roles/secretmanager.secretAccessor` - Only for specific secrets

**Scheduler Service Account** (`scheduler-sa`):
- `roles/run.invoker` - Only permission to invoke the specific function
- No other permissions

## Deployment

### Secure Deployment
Use the secure deployment script:

```bash
cd cloud_function
chmod +x setup_secrets.sh deploy_secure.sh
./deploy_secure.sh
```

This will:
1. Create secrets in Secret Manager
2. Configure service accounts with least privileges
3. Deploy function without public access
4. Configure Cloud Scheduler with OIDC authentication

### Testing

Test via Cloud Scheduler (authenticated):
```bash
gcloud scheduler jobs run descope-sync-daily \
  --location=us-central1 \
  --project=global-cloud-runtime
```

❌ **This will NOT work** (function is private):
```bash
curl -X POST https://function-url.com  # Returns 403 Forbidden
```

## Security Best Practices Followed

✅ **Secrets Management**: No secrets in code or environment variables  
✅ **Authentication**: All requests must be authenticated  
✅ **Authorization**: IAM policies enforce least privilege  
✅ **Audit Logging**: All access attempts are logged  
✅ **Network Security**: Function is not publicly accessible  
✅ **Service Accounts**: Dedicated accounts with minimal permissions  
✅ **Token Validation**: OIDC tokens verified on each request  

## Monitoring & Alerts

### View Access Logs
```bash
# View function logs
gcloud functions logs read descope-bigquery-sync \
  --gen2 \
  --region=us-central1 \
  --project=global-cloud-runtime \
  --limit=50

# Check for unauthorized access attempts
gcloud functions logs read descope-bigquery-sync \
  --gen2 \
  --region=us-central1 \
  --project=global-cloud-runtime \
  --filter="Unauthorized"
```

### Audit Secret Access
```bash
# View who accessed secrets
gcloud logging read "resource.type=secret_manager_secret" \
  --project=global-cloud-runtime \
  --limit=50
```

## Rotating Secrets

To rotate Descope credentials:

```bash
# Update the secret
echo -n "NEW_DESCOPE_MANAGEMENT_KEY" | \
  gcloud secrets versions add descope-management-key \
  --data-file=- \
  --project=global-cloud-runtime

# No need to redeploy - function will use new version automatically
```

## Incident Response

If credentials are compromised:

1. **Immediately rotate secrets** in Secret Manager
2. **Revoke old credentials** in Descope console
3. **Check audit logs** for unauthorized access:
   ```bash
   gcloud logging read "resource.labels.function_name=descope-bigquery-sync" \
     --project=global-cloud-runtime \
     --format=json
   ```
4. **Review IAM policies** to ensure no unauthorized access
5. **Monitor BigQuery** for unusual data patterns

## Comparison: Before vs After

| Feature | Before (Insecure) | After (Secure) |
|---------|------------------|----------------|
| **Public Access** | ✅ Anyone can call | ❌ Private only |
| **Secrets Storage** | Environment variables | Secret Manager |
| **Authentication** | None | OIDC tokens |
| **Authorization** | Public function | IAM-controlled |
| **Request Validation** | None | User-Agent + Token check |
| **Service Accounts** | Default (excessive) | Dedicated (least privilege) |
| **Audit Trail** | Limited | Full audit logs |
| **Secret Rotation** | Requires redeployment | Automatic |

## Compliance

This configuration helps meet common security compliance requirements:
- **SOC 2**: Access control, audit logging, encryption at rest
- **GDPR**: Data access controls, audit trails
- **ISO 27001**: Information security management
- **PCI DSS**: Restricted access to sensitive data

## Additional Recommendations

For production environments, consider:

1. **VPC Service Controls**: Add VPC perimeter for additional network isolation
2. **Cloud Armor**: Add DDoS protection if function becomes public
3. **Cloud Monitoring Alerts**: Alert on function failures or unauthorized access
4. **Binary Authorization**: Ensure only verified container images are deployed
5. **Workload Identity**: For enhanced service account security

