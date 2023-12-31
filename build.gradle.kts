plugins {
    java
}
group = "com.icloud"
version = "1.0-SNAPSHOT"

subprojects {
    apply(plugin = "java")
    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }
    repositories { mavenCentral() }
    dependencies {
        implementation("org.apache.hadoop:hadoop-client:3.1.3")
        implementation("org.apache.hive:hive-exec:3.1.3")
        implementation("org.apache.commons:commons-lang3:3.9")
        testImplementation(platform("org.junit:junit-bom:5.9.1"))
        testImplementation("org.junit.jupiter:junit-jupiter")
    }
}



tasks.test {
    useJUnitPlatform()
}