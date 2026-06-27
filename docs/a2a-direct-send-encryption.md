# A2A Direct Send Encryption

Direct-to-agent `a2a_send` messages can be encrypted on the bus while retaining
the existing Ed25519 signed-envelope trust model.

## Modes

Configure `A2A_DIRECT_ENCRYPTION`:

- `off`: always send signed plaintext.
- `opportunistic`: encrypt direct messages only when the recipient advertises
  decrypt capability in Redis. This is the default.
- `required`: direct sends fail with `direct_encryption_unavailable` unless the
  recipient advertises capability and its Redis pubkey can be used for encryption.

Topic broadcasts remain signed plaintext in every mode.

## Capability

A receiver that has local Ed25519 seed material publishes a TTL capability key:

```text
alloyium:a2a:direct-enc:<agent-id>
```

The value names the algorithm and is refreshed with the normal heartbeat. The key
is token-guarded on shutdown so a stopping process does not delete a successor's
capability.

Runtime knobs:

```text
A2A_DIRECT_ENCRYPTION=opportunistic
A2A_DIRECT_ENC_CAP_KEY_PREFIX=alloyium:a2a:direct-enc:
A2A_DIRECT_ENC_CAP_TTL_S=90
```

## Wire Format

Encrypted direct envelopes keep the normal signed envelope shape, but `body`
contains AES-GCM ciphertext and `enc` carries public encryption metadata:

```json
{
  "body": "<base64 ciphertext+tag>",
  "enc": {
    "alg": "x25519-ed25519-hkdf-sha256-aes-256-gcm-v1",
    "kid": "<recipient pubkey fingerprint>",
    "epk": "<base64 ephemeral X25519 public key>",
    "salt": "<base64 HKDF salt>",
    "iv": "<base64 AES-GCM IV>"
  }
}
```

The sender derives the recipient X25519 public key from the recipient Ed25519
Redis pubkey. The recipient derives its X25519 private key from its local Ed25519
seed. HKDF-SHA256 derives a per-message AES-256-GCM key from ephemeral-static
X25519 shared secret.

The Ed25519 envelope signature covers ciphertext and `enc` metadata. AES-GCM AAD
also binds envelope routing fields, thread/correlation, attrs, and the encryption
metadata.

## Receive Behavior

The receiver verifies signature and route first, then decrypts before local inbox
persistence and injection. The inbox database stores recipient plaintext in
`body`; `raw_envelope` remains the encrypted bus envelope. The portal masks
encrypted direct bus bodies as `[encrypted direct message]`.

Legacy signed plaintext direct envelopes are still accepted for rolling upgrades.

## Limitations

Sessions using only `externalSign` can sign without the core holding seed
material, but they cannot advertise decrypt capability unless the core is also
given local Ed25519 seed material through a future key-provider path.
