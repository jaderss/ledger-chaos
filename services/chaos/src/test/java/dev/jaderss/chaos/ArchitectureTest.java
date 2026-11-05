package dev.jaderss.chaos;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(packages = "dev.jaderss", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule actsFromOutside = noClasses()
            .that()
            .resideInAPackage("dev.jaderss.chaos..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage(
                    "dev.jaderss.ledger..",
                    "dev.jaderss.relay..",
                    "dev.jaderss.projector..",
                    "dev.jaderss.checker..",
                    "org.springframework..")
            .as("the runner is a client of the system: no service or checker code and no Spring");
}
