/**
 * src/encryption.ts  (hornpub-runner)
 *
 * Mirrors lib/server/encryption.ts in the frontend.
 * Both files MUST use the same algorithm, IV size, and packing format
 * so that tokens encrypted by the frontend API route are decryptable here.
 *
 * AES-256-GCM  |  IV: 12 bytes  |  Tag: 16 bytes  |  Encoding: base64url
 * Packed token: IV || authTag || ciphertext  (all in one base64url string)
 *
 * ENCRYPTION_SECRET must be set in the runner's environment (.env / systemd unit).
 * It must match the ENCRYPTION_SECRET used by the Next.js frontend.
 */

import {
  createCipheriv,
  createDecipheriv,
  randomBytes,
  createHash,
} from 'node:crypto';

// ─── Constants (must match frontend) ─────────────────────────────────────────
const ALGO      = 'aes-256-gcm' as const;
const IV_BYTES  = 12;
const TAG_BYTES = 16;

// ─── Key loading ──────────────────────────────────────────────────────────────

function loadKey(): Buffer {
  const raw = process.env.ENCRYPTION_SECRET;
  if (!raw || raw.trim().length === 0) {
    throw new Error(
      '[encryption] ENCRYPTION_SECRET is not set. ' +
      'Generate one with: openssl rand -hex 32  and add it to your .env'
    );
  }
  const trimmed = raw.trim();
  if (/^[0-9a-fA-F]{64}$/.test(trimmed)) {
    return Buffer.from(trimmed, 'hex');
  }
  // Derive 32 bytes from any passphrase via SHA-256
  return createHash('sha256').update(trimmed, 'utf8').digest();
}

let _key: Buffer | null = null;
function getKey(): Buffer {
  if (!_key) _key = loadKey();
  return _key;
}

// ─── Public API ───────────────────────────────────────────────────────────────

export function encryptString(plain: string): string {
  const key = getKey();
  const iv  = randomBytes(IV_BYTES);
  const cipher = createCipheriv(ALGO, key, iv);
  const encrypted = Buffer.concat([cipher.update(plain, 'utf8'), cipher.final()]);
  const authTag   = cipher.getAuthTag();
  return Buffer.concat([iv, authTag, encrypted]).toString('base64url');
}

export function decryptString(ciphertext: string): string {
  const buf = Buffer.from(ciphertext, 'base64url');
  if (buf.length < IV_BYTES + TAG_BYTES + 1) {
    throw new Error(
      `[encryption] Token too short (${buf.length} B). ` +
      'Possibly stored before encryption was enabled — re-save the wallet.'
    );
  }
  const iv      = buf.subarray(0, IV_BYTES);
  const authTag = buf.subarray(IV_BYTES, IV_BYTES + TAG_BYTES);
  const enc     = buf.subarray(IV_BYTES + TAG_BYTES);

  const decipher = createDecipheriv(ALGO, getKey(), iv);
  decipher.setAuthTag(authTag);

  // Throws "unable to authenticate data" if key is wrong or token was tampered with
  return Buffer.concat([decipher.update(enc), decipher.final()]).toString('utf8');
}

/** Quick sanity check — returns false for plaintext / legacy values */
export function isCiphertext(value: string): boolean {
  if (typeof value !== 'string' || value.length === 0) return false;
  if (!/^[A-Za-z0-9\-_]+=*$/.test(value)) return false;
  try {
    return Buffer.from(value, 'base64url').length >= IV_BYTES + TAG_BYTES + 1;
  } catch {
    return false;
  }
}
