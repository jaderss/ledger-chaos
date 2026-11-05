package dev.jaderss.checker;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(packages = "dev.jaderss", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule judgesFromOutside = noClasses()
            .that()
            .resideInAPackage("dev.jaderss.checker..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage(
                    "dev.jaderss.ledger..", "dev.jaderss.relay..", "dev.jaderss.projector..", "org.springframework..")
            .as("the checker judges the services from outside: no service code and no Spring");
}
