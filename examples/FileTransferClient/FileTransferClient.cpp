#ifndef MODULE_NAME
#define MODULE_NAME FileTransferClient
#endif

#include <core/core.h>
#include <iostream>
#include <fstream>

#undef EXTERNAL

using namespace WPEFramework;

namespace WPEFramework {

    class TextConnector : public Core::SocketDatagram {

        class FileUpdate : public Core::Thread {
            public:
                FileUpdate() = delete;
                FileUpdate(const FileUpdate&) = delete;
                FileUpdate& operator=(const FileUpdate&) = delete;

                FileUpdate(TextConnector& parent, const std::string& path)
                    : Core::Thread(Core::Thread::DefaultStackSize(), _T("FileUpdate"))
                    , _parent(parent)
                    , _storeFile(path, std::ios_base::app)
                {
                    Run();
                }
                virtual ~FileUpdate()
                {
                    Stop();
                }

                void Stop()
                {
                    _parent.Unlock();
                    Core::Thread::Block();
                    Wait(Core::Thread::INITIALIZED | Core::Thread::BLOCKED | Core::Thread::STOPPED, Core::infinite);
                }

            private:
                virtual uint32_t Worker()
                {
                    std::string line;
                    bool lineAvaliability;

                    do {
                        lineAvaliability = _parent.GetLine(line, Core::infinite);
                        // Store to the file
                        _storeFile << line;
                    } while (lineAvaliability == true);

                    return (Core::infinite);
                }

            private:
                TextConnector& _parent;
                std::ofstream _storeFile;
        };

        private:        
            static constexpr uint16_t MAX_BUFFER_LENGHT = 50*1024;

        public:
            TextConnector() = delete;
            TextConnector(const TextConnector& copy) = delete;
            TextConnector& operator=(const TextConnector&) = delete;

            TextConnector(const uint16_t port, const std::string& path)
                : Core::SocketDatagram(false, Core::NodeId("0.0.0.0", port), Core::NodeId(), 0, MAX_BUFFER_LENGHT)
                , _newLineSignal(false, true)
                , _adminLock()
                , _receivedQueue()
                , _update(*this, path)                 
            {
                Open(0);
            }

            virtual ~TextConnector()
            {
                _update.Stop();

                _receivedQueue.clear();
                Close(Core::infinite);
            }

            uint16_t ReceiveData(uint8_t *dataFrame, const uint16_t receivedSize) override
            {              
                std::string text(reinterpret_cast<const char*>(dataFrame), static_cast<size_t>(receivedSize));
                
                _adminLock.Lock();
                _receivedQueue.emplace_back(text);   
                _adminLock.Unlock();                         
                
                _newLineSignal.SetEvent(); 
                
                return receivedSize;
            }

            uint16_t SendData(uint8_t *dataFrame, const uint16_t maxSendSize) override
            {
                // Do not do anything here
                return 0;
            }

            void StateChange() override
            {
                if (IsOpen()) {
                    printf ("Observing file\n"); 
                } else {
                    printf("No longer observing file\n");
                }                
            }

            bool WaitForNewLine(const uint32_t waitTime)
            {
                uint32_t status = _newLineSignal.Lock(waitTime);
                _newLineSignal.ResetEvent();

                return (status == 0);
            }
        public: 
            void Unlock()
            {
                _newLineSignal.Unlock();
            }

            bool GetLine(std::string& text, const uint32_t waitTime)
            {
                static uint32_t size = 0;
                bool status = false;

                if (size == 0) {
                    status = WaitForNewLine(waitTime);
                }
                
                _adminLock.Lock();
                if (((status == true) && (_receivedQueue.size() > 0)) || ((status == false) && (_receivedQueue.size() > 0))) {
                    text = _receivedQueue.front();
                    _receivedQueue.pop_front();
                }
                size = static_cast<size_t>(_receivedQueue.size());
                _adminLock.Unlock();

                return (size > 0);
            }

        private:
            Core::Event _newLineSignal;
            Core::CriticalSection _adminLock;
            std::list<std::string> _receivedQueue;
            FileUpdate _update;
    };

    class Config : public Core::JSON::Container {
        public:
            Config(const Config &) = delete;
            Config &operator=(const Config &) = delete;

            class NetworkNode : public Core::JSON::Container
            {
                public:
                    NetworkNode()
                        : Core::JSON::Container(), Port(2201), Binding("0.0.0.0")
                    {
                        Add(_T("port"), &Port);
                        Add(_T("binding"), &Binding);
                    }
                    NetworkNode(const NetworkNode &copy)
                        : Core::JSON::Container(), Port(copy.Port), Binding(copy.Binding)
                    {
                        Add(_T("port"), &Port);
                        Add(_T("binding"), &Binding);
                    }
                    ~NetworkNode()
                    {
                    }
                    NetworkNode &operator=(const NetworkNode &RHS)
                    {
                        Port = RHS.Port;
                        Binding = RHS.Binding;
                        return (*this);
                    }

                public:
                    Core::JSON::DecUInt16 Port;
                    Core::JSON::String Binding;
            };

        public:
            Config()
                : FilePath(_T(EMPTY_STRING)), Source()
            {
                Add(_T("filepath"), &FilePath);
                Add(_T("destination"), &Source);
            }
            ~Config() override {}

        public:
            Core::JSON::String FilePath;
            NetworkNode Source;
    };

    void ShowMenu()
    {
        printf("Enter\n"
            "\t-interface : Interface to listen [obligatory]\n"
            "\t-port : UDP port [obligatory]\n"
            "\t-path : where the logs are stored [obligatory]\n"
            "\t-h : Help\n"
            "\t-q : Quit\n");
    }

    void ParseOptions(int argc, char** argv, Config& config)
    {
        int index = 1;

        while (index < argc) {
            if (strcmp(argv[index], "-interface") == 0) {
                std::string binding(argv[index + 1]);
                config.Source.Binding = binding;
                index++;
            } else if (strcmp(argv[index], "-port") == 0) {
                config.Source.Port = atoi(argv[index + 1]);
                index++;
            } else if (strcmp(argv[index], "-path") == 0) {
                std::string path(argv[index + 1]);
                config.FilePath = path;
                index++;
            } else if (strcmp(argv[index], "-h") == 0) {
                ShowMenu();
            }
            index++;
        }
    }
}

int main(int argc, char** argv)
{
    printf("Enter application.\n");

    Config config;
    ParseOptions(argc, argv, config);

    if ((config.Source.Port.IsSet() == false) ||
        (config.Source.Binding.IsSet() == false) ||
        (config.FilePath.IsSet() == false)) {

        Core::File configFile("FileTransferClient.json", false);
        if (configFile.Exists() && configFile.Open(true)) {
            config.FromFile(configFile);
            printf(_T("Get configuration from config file\n"));
        } else {
            printf(_T("Wrong parameter, use option -h to check required input arguments\n"));
            return (0);
        }
    }

    printf(_T("Interface IP   : %s\n"), config.Source.Binding.Value().c_str());
    printf(_T("Port           : %d\n"), config.Source.Port.Value());
    printf(_T("Path           : %s\n"), config.FilePath.Value().c_str());

    if (config.Source.Port.IsSet() &&
        config.FilePath.IsSet()) {

        TextConnector connector(config.Source.Port.Value(), config.FilePath.Value());

        int element;
        do {
            printf("\n>");
            element = toupper(getchar());

            switch (element) {
            case '?':
            case 'H':
                ShowMenu();
            }

            SleepMs(200);
        } while (element != 'Q');
    } 

    printf("Leaving app.\n");
    Core::Singleton::Dispose();

    return (0);
}
