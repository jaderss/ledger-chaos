package dev.jaderss.ledger;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(packages = "dev.jaderss.ledger", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule domainHasNoFrameworks = noClasses()
            .that()
            .resideInAPackage("..domain..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("org.springframework..", "java.sql..", "javax.sql..", "tools.jackson..", "jakarta..");

    @ArchTest
    static final ArchRule domainIsTheCore = noClasses()
            .that()
            .resideInAPackage("..domain..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("..app..", "..api..", "..persistence..", "..messaging..");

    @ArchTest
    static final ArchRule persistenceDoesNotKnowTheApi = noClasses()
            .that()
            .resideInAPackage("..persistence..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("..api..", "..app..", "..messaging..");

    @ArchTest
    static final ArchRule jdbiOwnsTheTransactions = noClasses()
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage(
                    "org.springframework.jdbc..", "org.springframework.dao..", "org.springframework.transaction..");

    @ArchTest
    static final ArchRule onlyPersistenceTalksToTheDatabase = noClasses()
            .that()
            .resideOutsideOfPackage("..persistence..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage("org.jdbi..");

    @ArchTest
    static final ArchRule messagingDoesNotKnowTheApi = noClasses()
            .that()
            .resideInAPackage("..messaging..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("..api..", "..app..", "..persistence..");
}
