// Chaos vocabulary for `cardToken`. See ../../idl/CARD_TOKENS.md.

export type ChargeOutcome =
  | { kind: 'succeed'; status: 'succeeded' | 'declined' | 'pending_review'; errorCode?: string }
  | { kind: 'fail'; retryable: boolean; errorCode: string; message: string }
  | { kind: 'sleep_then_succeed'; seconds: number }
  | { kind: 'hang' };

const RETRY_N_THEN_OK = /^tok_retry_(\d+)_then_ok$/;
const TIMEOUT_N_SECONDS = /^tok_timeout_(\d+)s$/;

/**
 * Interprets a chaos card token for a given attempt number (1-indexed, matching
 * `activityInfo().attempt`). Deterministic in the token and the attempt number so
 * retries of the same activity converge on the documented behavior.
 */
export function interpretCardToken(cardToken: string, attempt: number): ChargeOutcome {
  if (cardToken === 'tok_ok') {
    return { kind: 'succeed', status: 'succeeded' };
  }
  if (cardToken === 'tok_decline') {
    return { kind: 'succeed', status: 'declined' };
  }
  if (cardToken === 'tok_pending_review') {
    return { kind: 'succeed', status: 'pending_review' };
  }
  if (cardToken === 'tok_retry_forever') {
    return { kind: 'fail', retryable: true, errorCode: 'retry_forever', message: 'this card always fails retryably' };
  }
  if (cardToken === 'tok_fail_nonretryable') {
    return {
      kind: 'fail',
      retryable: false,
      errorCode: 'card_rejected',
      message: 'this card is permanently rejected',
    };
  }
  if (cardToken === 'tok_hang') {
    return { kind: 'hang' };
  }

  const retryMatch = RETRY_N_THEN_OK.exec(cardToken);
  if (retryMatch) {
    const n = Number(retryMatch[1]);
    if (attempt <= n) {
      return {
        kind: 'fail',
        retryable: true,
        errorCode: 'issuer_timeout',
        message: `simulated issuer timeout (attempt ${attempt} of ${n})`,
      };
    }
    return { kind: 'succeed', status: 'succeeded' };
  }

  const timeoutMatch = TIMEOUT_N_SECONDS.exec(cardToken);
  if (timeoutMatch) {
    return { kind: 'sleep_then_succeed', seconds: Number(timeoutMatch[1]) };
  }

  return { kind: 'succeed', status: 'declined', errorCode: 'unknown_card_token' };
}
