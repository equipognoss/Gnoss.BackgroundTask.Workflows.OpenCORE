FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build-env

RUN sed -i 's/\[openssl_init\]/# [openssl_init]/' /etc/ssl/openssl.cnf &&\
    printf "\n\n[openssl_init]\nssl_conf = ssl_sect" >> /etc/ssl/openssl.cnf &&\
    printf "\n\n[ssl_sect]\nsystem_default = ssl_default_sect" >> /etc/ssl/openssl.cnf &&\
    printf "\n\n[ssl_default_sect]\nMinProtocol = TLSv1\nCipherString = DEFAULT@SECLEVEL=0\n" >> /etc/ssl/openssl.cnf

RUN apt-get update && apt-get install -y --no-install-recommends curl

WORKDIR /app

COPY Gnoss.BackgroundTask.Workflows/*.csproj ./

RUN dotnet restore

COPY . ./

RUN dotnet publish Gnoss.BackgroundTask.Workflows/Gnoss.BackgroundTask.Workflows.csproj -c Release -o out

FROM mcr.microsoft.com/dotnet/aspnet:8.0

RUN sed -i 's/\[openssl_init\]/# [openssl_init]/' /etc/ssl/openssl.cnf &&\
    printf "\n\n[openssl_init]\nssl_conf = ssl_sect" >> /etc/ssl/openssl.cnf &&\
    printf "\n\n[ssl_sect]\nsystem_default = ssl_default_sect" >> /etc/ssl/openssl.cnf &&\
    printf "\n\n[ssl_default_sect]\nMinProtocol = TLSv1\nCipherString = DEFAULT@SECLEVEL=0\n" >> /etc/ssl/openssl.cnf

RUN apt-get update && apt-get install -y --no-install-recommends curl

WORKDIR /app

COPY --from=build-env /app/out .

RUN groupadd -g 2000 gnoss && useradd -u 2000 -g 2000 gnoss &&\
	mkdir -p config &&\
	chown -R gnoss:gnoss config && chmod -R 777 config
USER gnoss

ENTRYPOINT ["dotnet", "Gnoss.BackgroundTask.Workflows.dll"]
