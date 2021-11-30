/*
 * Copyright (c) 2020, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

import aQute.bnd.gradle.Bundle
import aQute.bnd.gradle.BundleTaskConvention
import com.github.vlsi.gradle.dsl.configureEach
import com.github.vlsi.gradle.gettext.GettextTask
import com.github.vlsi.gradle.gettext.MsgAttribTask
import com.github.vlsi.gradle.gettext.MsgFmtTask
import com.github.vlsi.gradle.gettext.MsgMergeTask
import com.github.vlsi.gradle.license.GatherLicenseTask
import com.github.vlsi.gradle.properties.dsl.props
import com.github.vlsi.gradle.release.dsl.dependencyLicenses
import com.github.vlsi.gradle.release.dsl.licensesCopySpec

plugins {
    id("biz.aQute.bnd.builder") apply false
    id("com.github.johnrengelman.shadow")
    id("com.github.lburgazzoli.karaf")
    id("com.github.vlsi.gettext")
    id("com.github.vlsi.gradle-extensions")
    id("com.github.vlsi.ide")
}

buildscript {
    repositories {
        // E.g. for biz.aQute.bnd.builder which is not published to Gradle Plugin Portal
        mavenCentral()
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8

    // Register AWS IAM authentication as a feature variant.
    registerFeature("awsIamAuthentication") {
        usingSourceSet(sourceSets["main"])
    }
}

val shaded by configurations.creating

val karafFeatures by configurations.creating {
    isTransitive = false
}

configurations {
    compileOnly {
        extendsFrom(shaded)
    }
    // Add shaded dependencies to test as well
    // This enables to execute unit tests with original (non-shaded dependencies)
    testImplementation {
        extendsFrom(shaded)
    }
}

val String.v: String get() = rootProject.extra["$this.version"] as String

dependencies {
    shaded(platform(project(":bom")))
    shaded("com.ongres.scram:client")

    "awsIamAuthenticationImplementation"("com.amazonaws:aws-java-sdk-rds:1.11.875")
    implementation("org.checkerframework:checker-qual")
    testImplementation("se.jiderhamn:classloader-leak-test-framework")
}

val skipReplicationTests by props()
val enableGettext by props()

if (skipReplicationTests) {
    tasks.configureEach<Test> {
        exclude("org/postgresql/replication/**")
        exclude("org/postgresql/test/jdbc2/CopyBothResponseTest*")
    }
}

tasks.configureEach<Test> {
    outputs.cacheIf("test results on the database configuration, so we can't cache it") {
        false
    }
}

val preprocessVersion by tasks.registering(org.postgresql.buildtools.JavaCommentPreprocessorTask::class) {
    baseDir.set(projectDir)
    sourceFolders.add("src/main/version")
}

ide {
    generatedJavaSources(
        preprocessVersion,
        preprocessVersion.get().outputDirectory.get().asFile,
        sourceSets.main
    )
}

// <editor-fold defaultstate="collapsed" desc="Gettext tasks">
tasks.configureEach<Checkstyle> {
    exclude("**/messages_*")
}

val update_pot_with_new_messages by tasks.registering(GettextTask::class) {
    sourceFiles.from(sourceSets.main.get().allJava)
    keywords.add("GT.tr")
}

val remove_obsolete_translations by tasks.registering(MsgAttribTask::class) {
    args.add("--no-obsolete") // remove obsolete messages
    // TODO: move *.po to resources?
    poFiles.from(files(sourceSets.main.get().allSource).filter { it.path.endsWith(".po") })
}

val add_new_messages_to_po by tasks.registering(MsgMergeTask::class) {
    poFiles.from(remove_obsolete_translations)
    potFile.set(update_pot_with_new_messages.map { it.outputPot.get() })
}

val generate_java_resources by tasks.registering(MsgFmtTask::class) {
    poFiles.from(add_new_messages_to_po)
    targetBundle.set("org.postgresql.translation.messages")
}

val generateGettextSources by tasks.registering {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Updates .po, .pot, and .java files in src/main/java/org/postgresql/translation"
    dependsOn(add_new_messages_to_po)
    dependsOn(generate_java_resources)
    doLast {
        copy {
            into("src/main/java")
            from(generate_java_resources)
            into("org/postgresql/translation") {
                from(update_pot_with_new_messages)
                from(add_new_messages_to_po)
            }
        }
    }
}

tasks.compileJava {
    if (enableGettext) {
        dependsOn(generateGettextSources)
    } else {
        mustRunAfter(generateGettextSources)
    }
}
// </editor-fold>

// <editor-fold defaultstate="collapsed" desc="Third-party license gathering">
val getShadedDependencyLicenses by tasks.registering(GatherLicenseTask::class) {
    configuration(shaded)
    extraLicenseDir.set(file("$rootDir/licenses"))
    overrideLicense("com.ongres.scram:common") {
        licenseFiles = "scram"
    }
    overrideLicense("com.ongres.scram:client") {
        licenseFiles = "scram"
    }
    overrideLicense("com.ongres.stringprep:saslprep") {
        licenseFiles = "stringprep"
    }
    overrideLicense("com.ongres.stringprep:stringprep") {
        licenseFiles = "stringprep"
    }
}

val renderShadedLicense by tasks.registering(com.github.vlsi.gradle.release.Apache2LicenseRenderer::class) {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Generate LICENSE file for shaded jar"
    mainLicenseFile.set(File(rootDir, "LICENSE"))
    failOnIncompatibleLicense.set(false)
    artifactType.set(com.github.vlsi.gradle.release.ArtifactType.BINARY)
    metadata.from(getShadedDependencyLicenses)
}

val shadedLicenseFiles = licensesCopySpec(renderShadedLicense)
// </editor-fold>

tasks.configureEach<Jar> {
    archiveBaseName.set("aws-postgresql-jdbc")
    manifest {
        attributes["Main-Class"] = "software.aws.rds.jdbc.postgresql.shading.org.postgresql.util.PGJDBCMain"
        attributes["Automatic-Module-Name"] = "org.postgresql.jdbc"
    }
}

tasks.shadowJar {
    configurations = listOf(shaded)
    dependsOn("jar")
    archiveClassifier.set("tmp")
    includeEmptyDirs = false

    into("META-INF") {
        dependencyLicenses(shadedLicenseFiles)
    }

    exclude("META-INF/maven/**")
    exclude("META-INF/LICENSE*")
    exclude("META-INF/NOTICE*")

    relocate("org.postgresql", "software.aws.rds.jdbc.postgresql.shading.org.postgresql") {
        exclude("org.postgresql.osgi.*")
    }
    relocate("com.ongres", "software.aws.rds.jdbc.postgresql.shading.com.ongres")
}

tasks.register<Jar>("cleanShadowJar") {

    dependsOn("shadowJar")
    includeEmptyDirs = false

    val shadowJar = tasks.shadowJar.get().archiveFile.get().asFile
    from(zipTree(shadowJar)) {
        include("META-INF/**")
        include("org/postgresql/osgi/**")
        include("software/**")
    }

    doLast {
        shadowJar.deleteRecursively()
    }
}

tasks.assemble {
    dependsOn("cleanShadowJar")
}

fun deleteUnshadedJar() {
    val unshadedJar = tasks.jar.get().archiveFile.get().asFile
    if (unshadedJar.exists()) {
        unshadedJar.deleteRecursively()
    }
}

tasks.configureEach<AbstractPublishToMaven> {
    doLast {
        deleteUnshadedJar()
    }
}

tasks.build {
    doLast {
        deleteUnshadedJar()
    }
}

val osgiJar by tasks.registering(Bundle::class) {
    archiveClassifier.set("osgi")
    from(tasks.named<Jar>("cleanShadowJar").map { zipTree(it.archiveFile) })
    withConvention(BundleTaskConvention::class) {
        bnd(
            """
            -exportcontents: !org.postgresql.shaded.*, org.postgresql.*, software.*
            -removeheaders: Created-By
            Bundle-Description: Amazon Web Services (AWS) JDBC Driver for PostgreSQL
            Bundle-DocURL: https://github.com/awslabs/aws-postgresql-jdbc
            Bundle-Vendor: Amazon Web Services (AWS)
            Import-Package: javax.sql, javax.transaction.xa, javax.naming, javax.security.sasl;resolution:=optional, *;resolution:=optional
            Bundle-Activator: org.postgresql.osgi.PGBundleActivator
            Bundle-SymbolicName: software.aws.rds
            Bundle-Name: Amazon Web Services (AWS) JDBC Driver for PostgreSQL
            Bundle-Copyright: Copyright Amazon.com Inc. or affiliates.
            Require-Capability: osgi.ee;filter:="(&(|(osgi.ee=J2SE)(osgi.ee=JavaSE))(version>=1.8))"
            Provide-Capability: osgi.service;effective:=active;objectClass=org.osgi.service.jdbc.DataSourceFactory
            """
        )
    }
}

karaf {
    features.apply {
        xsdVersion = "1.5.0"
        feature(closureOf<com.github.lburgazzoli.gradle.plugin.karaf.features.model.FeatureDescriptor> {
            name = "aws-postgresql-jdbc"
            description = "AWS PostgreSQL JDBC driver karaf feature"
            version = project.version.toString()
            details = "Java JDBC 4.2 (JRE 8+) driver for AWS PostgreSQL database"
            feature("transaction-api")
            includeProject = true
            bundle(project.group.toString(), closureOf<com.github.lburgazzoli.gradle.plugin.karaf.features.model.BundleDescriptor> {
                wrap = false
            })
            // List argument clears the "default" configurations
            configurations(listOf(karafFeatures))
        })
    }
}

// <editor-fold defaultstate="collapsed" desc="Trim checkerframework annotations from the source code">
val withoutAnnotations = layout.buildDirectory.dir("without-annotations").get().asFile

val sourceWithoutCheckerAnnotations by configurations.creating {
    isCanBeResolved = false
    isCanBeConsumed = true
}

val hiddenAnnotation = Regex(
    "@(?:Nullable|NonNull|PolyNull|MonotonicNonNull|RequiresNonNull|EnsuresNonNull|Initialized|UnknownInitialization|" +
            "Regex|" +
            "Pure|" +
            "KeyFor|" +
            "Positive|NonNegative|IntRange|" +
            "GuardedBy|UnderInitialization|" +
            "DefaultQualifier)(?:\\([^)]*\\))?")
val hiddenAnnotation2 = Regex("@(?:EnsuresNonNullIf)(?:\\([^)]*\\))?")
val hiddenImports = Regex("import org.checkerframework")

val removeTypeAnnotations by tasks.registering(Sync::class) {
    destinationDir = withoutAnnotations
    inputs.property("regexpsUpdatedOn", "2021-03-18")
    from(projectDir) {
        filteringCharset = `java.nio.charset`.StandardCharsets.UTF_8.name()
        filter { x: String ->
            x.replace(hiddenAnnotation2, "// $0")
                .replace(hiddenAnnotation, "/* $0 */")
                .replace(hiddenImports, "// $0")
        }
        include("src/**")
    }
}

(artifacts) {
    sourceWithoutCheckerAnnotations(withoutAnnotations) {
        builtBy(removeTypeAnnotations)
    }
}
// </editor-fold>

// <editor-fold defaultstate="collapsed" desc="Source distribution for building pgjdbc with minimal features">
val sourceDistribution by tasks.registering(Tar::class) {
    dependsOn(removeTypeAnnotations)
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Source distribution for building pgjdbc with minimal features"
    archiveClassifier.set("jdbc-src")
    archiveExtension.set("tar.gz")
    compression = Compression.GZIP
    includeEmptyDirs = false

    into(provider { archiveBaseName.get() + "-" + archiveVersion.get() + "-" + archiveClassifier.get() })

    from(rootDir) {
        include("build.properties")
        include("ssltest.properties")
        include("LICENSE")
        include("THIRD-PARTY-LICENSES")
        include("README.md")
    }

    val props = listOf(
        "pgjdbc.version",
        "junit4.version",
        "junit5.version",
        "commons-dbcp2.version",
        "aws-java-sdk-rds.version",
        "mockito-core.version",
        "classloader-leak-test-framework.version",
        "com.ongres.scram.client.version"
    ).associate { propertyName ->
        val value = project.findProperty(propertyName) as String
        inputs.property(propertyName, project.findProperty(propertyName))
        "%{$propertyName}" to value
    }

    from("reduced-pom.xml") {
        rename { "pom.xml" }
        filter {
            it.replace(Regex("%\\{[^}]+\\}")) {
                props[it.value] ?: throw GradleException("Unknown property in reduced-pom.xml: $it")
            }
        }
    }
    into("src/main/resources") {
        from(tasks.named<Jar>("cleanShadowJar").map {
            zipTree(it.archiveFile).matching {
                include("META-INF/MANIFEST.MF")
            }
        })
        into("META-INF") {
            dependencyLicenses(shadedLicenseFiles)
        }
    }
    into("src/main") {
        into("java") {
            from(preprocessVersion)
        }
        from("$withoutAnnotations/src/main") {
            exclude("resources/META-INF/LICENSE")
            exclude("checkstyle")
            exclude("*/org/postgresql/osgi/**")
            exclude("*/org/postgresql/sspi/NTDSAPI.java")
            exclude("*/org/postgresql/sspi/NTDSAPIWrapper.java")
            exclude("*/org/postgresql/sspi/SSPIClient.java")
        }
    }
    into("src/test") {
        from("$withoutAnnotations/src/test") {
            exclude("*/org/postgresql/test/osgi/**")
            exclude("**/*Suite*")
            exclude("*/org/postgresql/test/sspi/*.java")
            exclude("*/org/postgresql/replication/**")
        }
    }
    into("certdir") {
        from("$rootDir/certdir") {
            include("good*")
            include("bad*")
            include("Makefile")
            include("README.md")
            from("server") {
                include("root*")
                include("server*")
                include("pg_hba.conf")
            }
        }
    }
}
// </editor-fold>

val extraMavenPublications by configurations.getting

(artifacts) {
    extraMavenPublications(sourceDistribution)
    extraMavenPublications(osgiJar) {
        classifier = ""
    }
    extraMavenPublications(karaf.features.outputFile) {
        builtBy(tasks.named("generateFeatures"))
        classifier = "features"
    }
}
