/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    `java-library`
    id("com.github.johnrengelman.shadow")
}

val shaded by configurations.creating

configurations {
    compileOnly {
        extendsFrom(shaded)
    }
}

val adaptersForShade = listOf(
        ":babel", ":cassandra", ":druid", ":elasticsearch", ":file", ":geode", ":kafka", ":mongodb",
        ":pig", ":piglet", ":plus", ":redis", ":spark", ":splunk"
)
dependencies {
    shaded(project(":core"))
    for (p in adaptersForShade) {
        shaded(project(p))
    }
}

tasks {

    shadowJar {
        isZip64 = true
        archiveClassifier.set("shadow")
        configurations = listOf(shaded)
        exclude("META-INF/maven/**")
        exclude("META-INF/LICENSE*")
        exclude("META-INF/NOTICE*")
        listOf(
                "com.fasterxml.jackson",
                "com.google.protobuf",
                "org.apache.http",
                "org.apache.commons"
        ).forEach {
            relocate(it, "${project.group}.$it")
        }

    }

    jar {
        enabled = false
        dependsOn(shadowJar)
    }
}
