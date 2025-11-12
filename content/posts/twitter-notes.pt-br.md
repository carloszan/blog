+++
authors = ["Carlos Zansavio"]
title = "Notas do clone do Twitter"
date = "2025-09-08"
tags = [
    "clones",
]
categories = [
    "Twitter",
]
draft = true
+++

Eu quero construir um clone do Twitter. Atualmente, sou engenheiro de dados e, anteriormente, era engenheiro de software de backend.  
Não tenho todas as habilidades necessárias para fazer isso.  
Então, o que posso fazer é usar os clones que outras pessoas já fizeram e utilizar o código delas.

# Requisitos

- O aplicativo frontend e/ou mobile deve estar pronto.
    - Vou trabalhar apenas com a parte do backend.
- Começar de forma simples e tornar complexo posteriormente.
- Totalmente funcional no final, com CI/CD.

# Open sourced Twitter Clones

Lista:

| Name                                    | Github Source                                         | Demo                                         |
|-----------------------------------------|-------------------------------------------------------|----------------------------------------------|
| GithubShaban-Eissa/NextJS-Twitter-Clone | https://github.com/Shaban-Eissa/NextJS-Twitter-Clone  | https://next-js-twitter-clone-pi.vercel.app/ |
| ccrsxx/twitter-clone                    | https://github.com/ccrsxx/twitter-clone               | https://twitter-clone-ccrsxx.vercel.app/     |
| TheAlphamerc/flutter_twitter_clone      | https://github.com/TheAlphamerc/flutter_twitter_clone | Mobile app                                   |
| merikbest/twitter-spring-reactjs        | https://github.com/merikbest/twitter-spring-reactjs   | Frontend and Backend                         |


# Comparação

Link com a tabela de comparação:
https://docs.google.com/spreadsheets/d/1YP46ZUgBsgo2t-6hQmh3Pvz98VOVYJJJMpaq56ObF1E/edit?usp=sharing

## GithubShaban-Eissa/NextJS-Twitter-Clone

- Projeto interessante.
- Tem apenas 12 commits e um contribuidor.
- É feito em Javascript.
- Tem poucas features, porém features importantes.
- Não tem tela do usuário.
- OAuth com NextAuth.
- Next version: 12.2.5

## ccrsxx/twitter-clone
- Projeto maduro.
	- 902 stars
	- 3 contribuitors
	- 111 commits
- Tem a maioria das features necessárias.
- Typescript.
- Next version: 12.3.0

## TheAlphamerc/flutter_twitter_clone
- Projeto mobile escrito em Flutter.
- É o projeto com mais estrelas de Twitter Clone no Github (4200 estrelas).
- Não entendo nada de Flutter
## merikbest/twitter-spring-reactjs
- Projeto maduro.
- Escrito em React (Typescript) e Java.
- Microsserviços.
- Banco de dados: **Postgres**.
- Mídias no **S3**.

# Docker Build
```
docker build --platform linux/amd64,linux/arm64 -t carloszan/bemtevi:v1 
```
# API Definition

| Feature      | File                              | Possible HTTP API Endpoint | Schema                   |
| ------------ | --------------------------------- | -------------------------- | ------------------------ |
| Create tweet | src\components\input\input.tsx:75 | (POST) /api/v1/tweets<br>  | src\lib\types\tweet.ts:5 |
# To do

- [ ] Fix the need for having .env when building a new container. It blocks environment variables from outside.
- [ ] Username must be lowercase, when creating a new user.