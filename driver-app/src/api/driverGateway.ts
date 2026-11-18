const GATEWAY_BASE_URL = 'http://localhost:8000';

export type DeliveryStatus = 'ASSIGNED' | 'IN_TRANSIT' | 'DELIVERED' | 'FAILED_ATTEMPT';

export interface DriverManifest {
    orderId: string;
    clientId: string;
    address: string;
    routeId: string;
    assignedVehicle: string;
    assignedDriver: string;
    estimatedDistanceKm: number;
    estimatedEtaMinutes: number;
    deliveryStatus: DeliveryStatus;
    podNotes: string;
    updatedAt: string;
    failureReason?: string;
    signatureBase64?: string;
    photoProofBase64?: string;
    originLat?: number;
    originLng?: number;
    destLat?: number;
    destLng?: number;
}

export interface DeliveryUpdateOptions {
    failureReason?: string;
    signatureBase64?: string;
    photoProofBase64?: string;
}

// 1. Fetch active manifests -> API Gateway (:8000) -> Route & Driver Service (:8006)
export async function fetchDriverManifests(): Promise<DriverManifest[]> {
    const response = await fetch(`${GATEWAY_BASE_URL}/api/driver/manifests`);
    if (!response.ok) {
        throw new Error(`Gateway error (HTTP ${response.status})`);
    }
    return response.json();
}

// 2. Submit Status & Electronic Proof of Delivery -> API Gateway (:8000) -> Route & Driver Service (:8006)
export async function submitDeliveryUpdate(
    orderId: string,
    status: DeliveryStatus,
    podNotes: string,
    options: DeliveryUpdateOptions = {}
): Promise<DriverManifest> {
    const response = await fetch(`${GATEWAY_BASE_URL}/api/driver/manifests/${orderId}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status, podNotes, ...options }),
    });

    if (!response.ok) {
        throw new Error(`Failed to update delivery status (HTTP ${response.status})`);
    }
    return response.json();
}