package dev.jaderss.chaos;

import dev.jaderss.chaos.Config.Load;
import dev.jaderss.chaos.JsonLog.Body;
import dev.jaderss.chaos.JsonLog.Entry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

final class Workload {

    private static final String CURRENCY = "EUR";
    private static final int FUNDING_WORKER = -1;
    private static final Duration FUNDING_PAUSE = Duration.ofMillis(200);
    private static final Duration ERROR_PAUSE = Duration.ofMillis(50);

    record Settled(String key, Body body) {}

    private final LedgerClient ledger;
    private final JsonLog log;
    private final Load load;
    private final long seed;
    private final List<String> accounts = new ArrayList<>();
    private final List<Settled> settled = new ArrayList<>();
    private final ReentrantLock settledLock = new ReentrantLock();
    private String mint;

    Workload(LedgerClient ledger, JsonLog log, Load load, long seed) {
        this.ledger = ledger;
        this.log = log;
        this.load = load;
        this.seed = seed;
    }

    void setup() throws IOException, InterruptedException {
        mint = newId();
        ledger.createAccount(mint, CURRENCY, true);
        for (var i = 0; i < load.accounts(); i++) {
            var id = newId();
            ledger.createAccount(id, CURRENCY, load.overdraftEvery() > 0 && i % load.overdraftEvery() == 0);
            accounts.add(id);
        }
    }

    void fund() throws IOException, InterruptedException {
        for (var account : accounts) {
            var body = new Body(mint, account, load.fundingMinor(), CURRENCY);
            var key = newId();
            while (true) {
                var outcome = attempt(FUNDING_WORKER, key, body, false);
                if (outcome.ok()) {
                    break;
                }
                if (outcome.definite()) {
                    throw new IOException(
                            "funding %s refused: %d %s".formatted(account, outcome.status(), outcome.problem()));
                }
                Thread.sleep(FUNDING_PAUSE);
            }
        }
    }

    void run(Instant deadline) throws InterruptedException {
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var id = 0; id < load.workers(); id++) {
                var worker = id;
                workers.submit(() -> work(worker, deadline));
            }
        }
    }

    private void work(int worker, Instant deadline) {
        var random = new SplittableRandom(seed + worker);
        while (Instant.now().isBefore(deadline)) {
            var roll = random.nextDouble();
            if (roll < load.mismatchRate()) {
                var target = pickSettled(random);
                if (target.isPresent()) {
                    attempt(worker, target.get().key(), target.get().body().plusOne(), true);
                } else {
                    send(worker, newId(), randomBody(random));
                }
            } else if (roll < load.mismatchRate() + load.duplicateRate()) {
                duplicate(worker, newId(), randomBody(random));
            } else {
                send(worker, newId(), randomBody(random));
            }
        }
    }

    private void duplicate(int worker, String key, Body body) {
        try (var pair = Executors.newVirtualThreadPerTaskExecutor()) {
            pair.submit(() -> send(worker, key, body));
            pair.submit(() -> send(worker, key, body));
        }
    }

    Body randomBody(SplittableRandom random) {
        var from = random.nextInt(accounts.size());
        var to = random.nextInt(accounts.size() - 1);
        if (to >= from) {
            to++;
        }
        return new Body(accounts.get(from), accounts.get(to), 1 + random.nextLong(load.amountMaxMinor()), CURRENCY);
    }

    void send(int worker, String key, Body body) {
        for (var i = 0; i <= load.retries(); i++) {
            var outcome = attempt(worker, key, body, false);
            if (outcome.definite()) {
                if (outcome.ok()) {
                    remember(new Settled(key, body));
                }
                return;
            }
        }
    }

    private Outcome attempt(int worker, String key, Body body, boolean mismatch) {
        log.write(Entry.invoke(worker, key, body, mismatch));
        var outcome = ledger.transfer(key, body, load.clientTimeout());
        log.write(Entry.result(worker, key, mismatch, outcome));
        if (!outcome.definite() && outcome.error() != null) {
            pause();
        }
        return outcome;
    }

    private static void pause() {
        try {
            Thread.sleep(ERROR_PAUSE);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    private void remember(Settled transfer) {
        settledLock.lock();
        try {
            settled.add(transfer);
        } finally {
            settledLock.unlock();
        }
    }

    private Optional<Settled> pickSettled(SplittableRandom random) {
        settledLock.lock();
        try {
            return settled.isEmpty() ? Optional.empty() : Optional.of(settled.get(random.nextInt(settled.size())));
        } finally {
            settledLock.unlock();
        }
    }

    List<String> accounts() {
        return List.copyOf(accounts);
    }

    int settledCount() {
        settledLock.lock();
        try {
            return settled.size();
        } finally {
            settledLock.unlock();
        }
    }

    private static String newId() {
        return UUID.randomUUID().toString();
    }
}
