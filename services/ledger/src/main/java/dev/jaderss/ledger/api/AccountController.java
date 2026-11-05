package dev.jaderss.ledger.api;

import dev.jaderss.ledger.domain.Account;
import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.AccountNotFoundException;
import dev.jaderss.ledger.domain.Money;
import dev.jaderss.ledger.persistence.LedgerDatabase;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/accounts")
class AccountController {

    private final LedgerDatabase database;

    AccountController(LedgerDatabase database) {
        this.database = database;
    }

    @PostMapping
    ResponseEntity<Object> create(@Valid @RequestBody AccountRequest request) {
        var id = new AccountId(request.id());
        var account = new Account(
                id, InvalidRequestException.parse(() -> Money.of(0, request.currency())), request.allowOverdraft());
        if (database.withStores(stores -> stores.accounts().create(account))) {
            return ResponseEntity.created(URI.create("/accounts/" + id)).body(AccountResponse.from(find(id)));
        }
        var existing = find(id);
        if (existing.balance().currency().getCurrencyCode().equals(request.currency())
                && existing.allowOverdraft() == request.allowOverdraft()) {
            return ResponseEntity.ok(AccountResponse.from(existing));
        }
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Problems.of(
                        HttpStatus.CONFLICT, "account-conflict", "account " + id + " exists with other settings"));
    }

    @GetMapping("/{id}")
    AccountResponse get(@PathVariable UUID id) {
        return AccountResponse.from(find(new AccountId(id)));
    }

    private Account find(AccountId id) {
        return database.withStores(stores -> stores.accounts().find(id))
                .orElseThrow(() -> new AccountNotFoundException(id));
    }
}
