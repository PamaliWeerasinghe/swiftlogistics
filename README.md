# SwiftLogistics

SwiftLogistics is a distributed logistics and order-fulfillment platform that demonstrates a microservice saga, asynchronous RabbitMQ messaging, and integration with mock legacy CMS and WMS systems.

## Features

- Client order submission and live order tracking
- Driver manifests, delivery status updates, rerouting, and proof of delivery
- REST, SOAP/XML, and TCP-style protocol adapters
- RabbitMQ event-driven communication
- Saga orchestration with compensation and dead-letter handling
- Server-Sent Events (SSE) for live client updates
- Configurable API Gateway token validation
- React and TypeScript client and driver applications

## Architecture

```text
Client Portal ─┐                         ┌─ CMS Adapter ── Mock CMS
               ├─ API Gateway ─┐        ├─ WMS Adapter ── Mock WMS
Driver App ────┘                ├─ RabbitMQ ── ROS Adapter ── Mock ROS
                                └─ Order Intake/Processing
                                     └─ Route/Driver ── Tracking/SSE
```

| Component | Port | Responsibility |
| --- | ---: | --- |
| API Gateway | 8000 | Authentication, routing, CORS, and gateway endpoints |
| Order Intake | 8001 | Accepts and stores incoming orders |
| Order Processing | 8002 | Runs the fulfillment saga and compensation flow |
| CMS Adapter | 8003 | Converts order events to the CMS integration contract |
| WMS Adapter | 8004 | Converts dispatch events for the warehouse system |
| ROS Adapter | 8005 | Sends route requests to the ROS mock service |
| Route & Driver Service | 8006 | Manages manifests, locations, reroutes, and delivery status |
| Notification & Tracking Service | 8007 | Stores tracking data and streams SSE notifications |
| Mock CMS | 8080 | SOAP/XML and REST mock billing/mainframe system |
| Mock WMS | 8081, 9090 | REST API and TCP dispatch endpoint |
| Mock ROS | 8082 | Route optimization REST service |
| Client Portal | 5173 | Customer order and tracking UI |
| Driver App | 5174 | Driver manifest and delivery UI |
| RabbitMQ AMQP | 5672 | Service messaging |
| RabbitMQ Management | 15672 | Queue and dead-letter administration |

## Technology stack

- Java 17/21 and Spring Boot
- Spring Cloud Gateway
- RabbitMQ
- React 19, TypeScript, Vite, Tailwind CSS, and Leaflet
- Maven Wrapper
- Docker Compose

## Prerequisites

- JDK 21 (recommended for the complete service set)
- Maven Wrapper is included; a system Maven installation is not required
- Node.js and npm
- Docker Desktop or Docker Engine with Compose
- Windows PowerShell (for `start-all-services.ps1`)

## Quick start

### 1. Start RabbitMQ

From the repository root:

```powershell
docker compose up -d rabbitmq
```

Open the management console at <http://localhost:15672>.

- Username: `guest`
- Password: `guest`

### 2. Start the Java services

On Windows:

```powershell
.\start-all-services.ps1
```

This opens one PowerShell window per Spring service. Wait until the services finish starting before submitting an order.

You can also start an individual service manually:

```powershell
cd order_intake
.\mvnw.cmd spring-boot:run
```

### 3. Start the React applications

Client portal:

```powershell
cd client-portal
npm install
npm run dev -- --host 0.0.0.0
```

Driver app in a separate terminal:

```powershell
cd driver-app
npm install
npm run dev -- --host 0.0.0.0
```

Open:

- Client portal: <http://localhost:5173>
- Driver app: <http://localhost:5174>

## Try the normal workflow

1. Open the client portal.
2. Enter any non-empty username and password, choose `CLIENT`, and sign in.
3. Submit an order with a client ID and delivery address.
4. Track the order status while the saga invokes CMS, WMS, ROS, and route services.
5. Open the driver app and sign in with any non-empty credentials and the `DRIVER` role.
6. View the generated manifest and update delivery status or proof of delivery.

The local authentication service accepts demo credentials; validation is enabled by the gateway but no fixed account database is used.

## Useful endpoints

| Endpoint | Purpose |
| --- | --- |
| `POST /api/auth/login` | Obtain a demo gateway token |
| `POST /api/v1/orders` | Create an order |
| `GET /api/v1/orders` | List orders |
| `GET /api/tracking/orders/{orderId}` | Get tracking state |
| `GET /api/tracking/stream` | Subscribe to live tracking events |
| `GET /api/driver/manifests` | List driver manifests |
| `POST /api/driver/manifests/{orderId}/status` | Update delivery status |
| `POST /api/mock-cms/toggle` | Toggle mock CMS availability |
| `POST /api/mock-wms/toggle` | Toggle mock WMS availability |
| `POST /api/mock-ros/toggle` | Toggle mock ROS availability |

Most gateway endpoints are available through `http://localhost:8000`; mock-system administration endpoints are exposed on their own ports.

## Testing outages and recovery

1. Open the mock CMS status panel or call `POST http://localhost:8080/api/mock-cms/toggle`.
2. Submit a new order and observe the failed stage and tracking updates.
3. Restore CMS and submit another order, or trigger compensation from the mock CMS endpoint.
4. Repeat with WMS on port `8081` and ROS on port `8082`.
5. Inspect RabbitMQ queues and dead-lettered messages at <http://localhost:15672>.

The gateway token filter can be disabled for local development by setting:

```yaml
security:
  enforce-token: false
```

in `api-gateway/src/main/resources/application.yml`.

## Project layout

```text
api-gateway/                 Spring Cloud API Gateway
order_intake/                Order ingestion
order_processing/            Saga orchestration
cms_adapter/                 CMS protocol adapter
wms_adapter/                 WMS protocol adapter
ros-adapter/                 ROS protocol adapter
route-driver-service/        Delivery and manifest management
notification-tracking-service SSE and tracking service
CMS/                         Mock CMS system
mock-wms-system/             Mock WMS REST and TCP system
mock-ros-cloud/              Mock ROS system
client-portal/               React customer application
driver-app/                  React driver application
```

## Stopping the platform

Stop RabbitMQ and remove its containers:

```powershell
docker compose down
```

Stop each Spring service and React dev server with `Ctrl+C` in its terminal.

## Notes

- Persistent mock-system JSON data is stored under `./data/*.json` and is ignored by Git.
- Frontend applications communicate with the API Gateway on `http://localhost:8000`.
- RabbitMQ uses `guest/guest` for local development only.
