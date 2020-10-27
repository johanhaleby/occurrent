package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.CollectionOptions
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.collectionExists
import org.springframework.data.mongodb.core.createCollection
import javax.annotation.PostConstruct

@Configuration
class InitializeEndedGameCappedCollection {

    @Autowired
    lateinit var mongoOperations: MongoOperations


    @PostConstruct
    fun createCappedEndedGameOverviewMongoDBCollection() {
        if (!mongoOperations.collectionExists<EndedGameOverviewMongoDTO>()) {
            mongoOperations.createCollection<EndedGameOverviewMongoDTO>(CollectionOptions.empty().capped().maxDocuments(10).size(1_000_000))
        }
    }
}