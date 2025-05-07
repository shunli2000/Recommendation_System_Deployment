pipeline {
    agent any

    environment {
        VENV_DIR = "mlip-env"  // Change if your virtual environment has a different name
        PIP_CACHE_DIR = "/var/lib/jenkins/.pip_cache"
    }

    triggers {
        githubPush()
        cron('H H */3 * *')
    }

    stages {
        stage('Checkout') {
            steps {
                git branch: 'main', 
                    credentialsId: 'f36fa6e2-4bbc-4fa8-be73-adc08cd22609',
                    url: 'git@github.com:cmu-seai/group-project-s25-the-inceptionists.git'
            }
        }

        stage('Setup Virtual Environment') {
            steps {
                sh '''
                #!/bin/bash
                # Check if virtual environment exists, if not, create it
                if [ ! -d "$VENV_DIR" ]; then
                    echo "Creating virtual environment..."
                    python3 -m venv $VENV_DIR
                else
                    echo "Virtual environment already exists."
                fi
                
                # Activate virtual environment and upgrade pip
                . $VENV_DIR/bin/activate
                pip install --upgrade pip
                deactivate
                '''
            }
        }

        stage('Install Dependencies') {
            steps {
                sh '''
                #!/bin/bash
                . $VENV_DIR/bin/activate
                pip install --cache-dir ~/.pip_cache -r requirements.txt
                deactivate
                '''
            }
        }

        stage('Run Offline Evaluation') {
            steps {
                sh '''
                #!/bin/bash
                . $VENV_DIR/bin/activate
                python3 offline_evaluation.py
                deactivate
                '''
            }
        }

        stage('Run Online Evaluation') {
            steps {
                sh '''
                #!/bin/bash
                . $VENV_DIR/bin/activate
                python3 online_evaluation.py
                deactivate
                '''
            }
        }

        stage('Consume Data from Kafka') {
            steps {
                echo 'Starting Kafka data consumer...'
                sh '''
                #!/bin/bash
                . $VENV_DIR/bin/activate
                python3 kafka_consumer.py
                deactivate
                '''
            }
        }

        stage('Run Tests') {
            steps {
                sh '''
                #!/bin/bash
                . $VENV_DIR/bin/activate
                pytest --junitxml=report.xml
                deactivate
                '''
            }
        }

        stage('Run Coverage') {
            steps {
                sh '''
                #!/bin/bash
                . $VENV_DIR/bin/activate
                python3 coverage_test.py
                deactivate
                '''
            }
        }

        stage('Run Schema Validation') {
            steps {
                sh '''
                #!/bin/bash
                . $VENV_DIR/bin/activate
                python3 schema_validation.py
                deactivate
                '''
            }
        }

        stage('Run Data Drift Checking') {
            steps {
                sh '''
                #!/bin/bash
                . $VENV_DIR/bin/activate
                python3 drift_detection.py
                deactivate
                '''
            }
        }

        stage('Run DB write') {
            steps {
                sh '''
                #!/bin/bash
                . $VENV_DIR/bin/activate
                python3 db_writer.py
                deactivate
                '''
            }
        }

        stage('Deploy Using Docker Compose') {
            steps {
                script {
                    echo 'Deploying Using Docker Compose'
                    sh '''
                    # Stop and remove any containers related to this Compose project (by name)
                    docker ps -q --filter "name=mlip-test-pipeline" | xargs -r docker stop
                    docker ps -a -q --filter "name=mlip-test-pipeline" | xargs -r docker rm -f

                    # Prune networks to avoid "already exists" conflicts
                    docker network prune -f

                    # Clean up Compose environment
                    docker-compose down --remove-orphans

                    # Rebuild everything cleanly
                    docker-compose build --no-cache

                    # Start the services in detached mode
                    docker-compose up -d
                    '''
                }
            }
        }


        stage('Run Retraining') {
            steps {
                sh '''
                #!/bin/bash
                . $VENV_DIR/bin/activate
                cd ML
                python3 retrain.py
                deactivate
                '''
            }
        }

        stage('Archive Test Results') {
            steps {
                junit 'report.xml'
            }
        }

        stage('Archive Coverage Report') {
            steps {
                archiveArtifacts artifacts: 'coverage.xml', fingerprint: true
            }
        }
    }

    post {
        success {
            echo "All tests passed successfully!"
        }
        failure {
            echo "Some tests failed. Check the logs."
        }
        always {
            // Clean workspace only if necessary
            cleanWs(patterns: [[pattern: '**/temp/*', type: 'INCLUDE']])
        }
    }
}
