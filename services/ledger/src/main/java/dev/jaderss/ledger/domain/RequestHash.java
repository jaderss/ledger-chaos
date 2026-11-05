package dev.jaderss.ledger.domain;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;

public final class RequestHash {

    private final byte[] bytes;

    private RequestHash(byte[] bytes) {
        this.bytes = bytes.clone();
    }

    public static RequestHash of(AccountId from, AccountId to, Money amount) {
        var canonical = String.join(
                "\n",
                from.toString(),
                to.toString(),
                Long.toString(amount.amountMinor()),
                amount.currency().getCurrencyCode());
        return new RequestHash(sha256().digest(canonical.getBytes(StandardCharsets.UTF_8)));
    }

    public static RequestHash fromBytes(byte[] bytes) {
        return new RequestHash(bytes);
    }

    public byte[] bytes() {
        return bytes.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof RequestHash that && MessageDigest.isEqual(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return HexFormat.of().formatHex(bytes);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
