const GATEWAY_BASE_URL = 'http://localhost:8000';

export interface OrderRequest {
    orderId: string;
    clientId: string;
    address: string;
}

export interface TrackingHistoryItem {
    stage: string;
    details: string;
    timestamp: string;
}

export interface TrackedOrder {
    orderId: string;
    clientId: string;
    address?: string;
    currentStage: string;
    currentRank: number;
    latestDetails: string;
    updatedAt: string;
    assignedDriver?: string;
    assignedVehicle?: string;
    originLat?: number;
    originLng?: number;
    destLat?: number;
    destLng?: number;
    currentLat?: number;
    currentLng?: number;
    history: TrackingHistoryItem[];
}

export interface NotificationAlert {
    timestamp: string;
    orderId: string;
    clientId: string;
    stage: string;
    message: string;
}

export interface TrackingStreamEvent {
    type?: string;
    order?: TrackedOrder;
    notification?: NotificationAlert;
    orders?: Record<string, TrackedOrder>;
    notifications?: NotificationAlert[];
}

// 1. Submit Order -> API Gateway (:8000) -> Order Intake (:8001)
export async function submitOrder(payload: OrderRequest): Promise<void> {
    const response = await fetch(`${GATEWAY_BASE_URL}/api/v1/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });

    if (!response.ok) {
        throw new Error(`Order submission failed (HTTP ${response.status})`);
    }
}

// 2. Fetch Live Order Progress -> API Gateway (:8000) -> Tracking Service (:8007)
export async function fetchTrackedOrders(): Promise<TrackedOrder[]> {
    const response = await fetch(`${GATEWAY_BASE_URL}/api/tracking/orders`);
    if (!response.ok) return [];
    return response.json();
}

// 3. Fetch Customer SMS/Email Notifications -> API Gateway (:8000) -> Tracking Service (:8007)
export async function fetchNotifications(): Promise<NotificationAlert[]> {
    const response = await fetch(`${GATEWAY_BASE_URL}/api/tracking/notifications`);
    if (!response.ok) return [];
    return response.json();
}

export function openTrackingStream(onUpdate: (event: TrackingStreamEvent) => void): EventSource {
    const connection = new EventSource(`${GATEWAY_BASE_URL}/api/tracking/stream`);
    connection.onmessage = (event) => {
        try {
            onUpdate(JSON.parse(event.data));
        } catch {
            onUpdate({ type: 'tracking-update', order: undefined, notification: undefined });
        }
    };
    connection.addEventListener('tracking-update', (event: MessageEvent) => {
        try {
            onUpdate(JSON.parse(event.data));
        } catch {
            onUpdate({ type: 'tracking-update', order: undefined, notification: undefined });
        }
    });
    connection.onerror = () => {
        connection.close();
    };
    return connection;
}